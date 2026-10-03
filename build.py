import asyncio
import re
import os
import aiohttp

# 1. 基础配置
SOURCES_FILE = "sources.txt"
OUTPUT_M3U = "dist/live.m3u"
OUTPUT_TXT = "dist/live.txt"
EPG_URL = "http://epg.51zmt.top:8000/e.xml"

# 【测速与并发配置】
TIMEOUT_SECONDS = 5.0       # 总超时时间
CONNECT_TIMEOUT = 3.0       # 建连超时
MAX_CONCURRENT_CHECKS = 60  # 并发测速数
MAX_PER_CHANNEL = 8         # 每个频道保留的最佳线路数
KEEP_IPV6 = True            # 如不支持 IPv6 请设为 False

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

PAY_TV_KEYWORDS = [
    "风云足球", "风云剧场", "风云音乐", "兵器科技", "女性时尚", "文化精品", 
    "高尔夫网球", "第一剧场", "央视台球", "世界地理", "电视指南", "怀旧剧场",
    "发现之旅", "中学生", "环球旅游", "摄影", "早期教育", "超级体育", "经典剧场"
]

FOREIGN_BLACK_LIST = [
    "BBC", "CNN", "HBO", "FOX", "NHK", "KBS", "TVB", "ALJAZAERA", "DW",
    "DISCOVERY", "NATIONAL GEOGRAPHIC", "STAR MOVIES", "BLOOMBERG"
]

def clean_channel_name(name):
    """标准化频道名称"""
    if not name:
        return None

    name_upper = name.upper()
    for foreign in FOREIGN_BLACK_LIST:
        if foreign in name_upper:
            return None

    # 1. 专门匹配 CCTV 频道 (例: CCTV1, CCTV-1, CCTV13新闻, CCTV 4K, CCTV5+)
    cctv_match = re.search(r'CCTV[-_\s]*(\d+\+?|13|NEWS|5\+|4K|8K)', name, re.IGNORECASE)
    if cctv_match:
        num = cctv_match.group(1).upper()
        if num == "NEWS":
            num = "13"
        return f"CCTV-{num}"

    if "CCTV" in name_upper:
        name = re.sub(r'[\(\[\（\【\-\_\s]*(1080[pP]|720[pP]|4[kK]|[hH][dD]|高清|超清|标清|测试|专线)[\)\]\）\】]*', '', name)
        return name.strip()

    # 2. 清理常见后缀
    pattern = r'[\(\[\（\【\-\_\s]*(1080[pP]|720[pP]|4[kK]|2[kK]|[hH][dD]|[fF][hH][dD]|[sS][dD]|高清|超清|标清|原生|备用|测试|专线|码率|[hH]264|[hH]265|[hH][eE][vV][cC])[\)\]\）\】]*'
    name = re.sub(pattern, '', name, flags=re.IGNORECASE)

    name = name.strip(' -_')
    return name if name else None

def get_channel_group(name):
    """频道分组"""
    if name.startswith("CCTV"):
        return "央视频道"
    for pay_kw in PAY_TV_KEYWORDS:
        if pay_kw in name:
            return "付费数字"
    if any(ws in name for ws in ["卫视", "凤凰"]):
        return "卫视精品"
    if any(movie_kw in name for movie_kw in ["电影", "影院", "剧场", "轮播", "系列", "周星驰", "林正英", "漫威", "综合"]):
        return "影视轮播"
    return "地方频道"

async def check_stream(session, semaphore, url):
    """兼容 HEAD/GET 的测速探测"""
    if not url.startswith(("http://", "https://")):
        return False, 999

    async with semaphore:
        try:
            start_time = asyncio.get_event_loop().time()
            timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS, connect=CONNECT_TIMEOUT)
            
            # 先尝试 HEAD
            try:
                async with session.head(url, headers=headers, timeout=timeout, allow_redirects=True, ssl=False) as resp:
                    if resp.status in [200, 206, 301, 302]:
                        latency = asyncio.get_event_loop().time() - start_time
                        return True, latency
            except Exception:
                pass

            # HEAD 失败后再尝试 GET 读取首包
            async with session.get(url, headers=headers, timeout=timeout, allow_redirects=True, ssl=False) as resp:
                if resp.status in [200, 206]:
                    chunk = await resp.content.read(128)
                    if chunk:
                        latency = asyncio.get_event_loop().time() - start_time
                        return True, latency
        except Exception:
            pass
        return False, 999

def parse_playlist_content(content):
    """同时兼容 M3U 与 TXT 格式解析"""
    channels = []
    lines = content.splitlines()
    current_info = ""

    for line in lines:
        line = line.strip()
        if not line or line.startswith("#EXTM3U"):
            continue

        if line.startswith("#EXTINF:"):
            current_info = line
        elif not line.startswith("#"):
            raw_name = ""
            url = line

            if current_info:
                # M3U 格式解析
                name_match = re.search(r',([^,]+)$', current_info)
                raw_name = name_match.group(1).strip() if name_match else ""
                current_info = ""
            elif "," in line:
                # TXT 格式解析 (例: CCTV-1,http://...)
                parts = line.split(",", 1)
                raw_name = parts[0].strip()
                url = parts[1].strip()

            if raw_name and url.startswith(("http://", "https://")):
                clean_name = clean_channel_name(raw_name)
                if clean_name:
                    group = get_channel_group(clean_name)
                    channels.append({
                        "name": clean_name,
                        "group": group,
                        "url": url
                    })
    return channels

async def fetch_source(session, url):
    """抓取单上游源"""
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15), ssl=False) as resp:
            if resp.status == 200:
                text = await resp.text(errors='ignore')
                return parse_playlist_content(text)
    except Exception as e:
        print(f"读取上游失败 {url}: {e}")
    return []

async def main():
    if not os.path.exists(SOURCES_FILE):
        print(f"错误: 找不到 {SOURCES_FILE}")
        return

    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        sources = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    # 关闭 SSL 检查以提高打通率
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_CHECKS, ssl=False)
    
    all_channels = []
    async with aiohttp.ClientSession(connector=connector) as session:
        print(f"正在并发抓取 {len(sources)} 个上游源...")
        fetch_tasks = [fetch_source(session, src) for src in sources]
        source_results = await asyncio.gather(*fetch_tasks)
        
        for res in source_results:
            all_channels.extend(res)

        print(f"抓取完成，共搜集到 {len(all_channels)} 条直播流。开始测速筛选...")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)
        seen_urls = set()

        tasks = []
        for ch in all_channels:
            if ch["url"] in seen_urls:
                continue
            seen_urls.add(ch["url"])
            tasks.append((ch, check_stream(session, semaphore, ch["url"])))

        results = await asyncio.gather(*[t[1] for t in tasks])
        
        valid_channels = []
        for (ch, _), (is_valid, latency) in zip(tasks, results):
            if is_valid:
                ch["latency"] = latency
                valid_channels.append(ch)

    print(f"测速完成，共获得 {len(valid_channels)} 条有效线路！正在按延迟排序...")

    # 按频道聚合并选取延迟最低的前 N 条
    channel_groups = {}
    for ch in valid_channels:
        key = ch["name"]
        if key not in channel_groups:
            channel_groups[key] = []
        channel_groups[key].append(ch)

    final_channels = []
    for name, lines in channel_groups.items():
        lines.sort(key=lambda x: x["latency"])
        final_channels.extend(lines[:MAX_PER_CHANNEL])

    final_channels.sort(key=lambda x: (x["group"], x["name"]))

    os.makedirs("dist", exist_ok=True)

    # 写入 M3U
    with open(OUTPUT_M3U, "w", encoding="utf-8") as f:
        f.write(f'#EXTM3U x-tvg-url="{EPG_URL}"\n')
        for ch in final_channels:
            info = f'#EXTINF:-1 group-title="{ch["group"]}",{ch["name"]}'
            f.write(f'{info}\n{ch["url"]}\n')

    # 写入 TXT
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        current_grp = ""
        for ch in final_channels:
            if ch["group"] != current_grp:
                current_grp = ch["group"]
                f.write(f'{current_grp},#genre#\n')
            f.write(f'{ch["name"]},{ch["url"]}\n')

    print(f"任务完成！成功保留 {len(final_channels)} 条线路，涵盖 {len(channel_groups)} 个频道。")

if __name__ == "__main__":
    asyncio.run(main())
