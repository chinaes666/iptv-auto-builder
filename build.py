import asyncio
import re
import os
import aiohttp

# 1. 基础配置
SOURCES_FILE = "sources.txt"
OUTPUT_M3U = "dist/live.m3u"
OUTPUT_TXT = "dist/live.txt"
EPG_URL = "http://epg.51zmt.top:8000/e.xml"

# 【测速参数调优】
TIMEOUT_SECONDS = 4.5       # 稍微放宽超时，给缓冲预留时间，提升留存率
CONNECT_TIMEOUT = 2.5       # 连接建立超时
MAX_CONCURRENT_CHECKS = 40  # 并发数
KEEP_IPV6 = True            # 如果播放设备不支持 IPv6，请改为 False

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

async def check_stream(session, semaphore, url):
    """验证单个直播源连通性及响应状态"""
    if not url.startswith(("http://", "https://")):
        return False

    # 是否跳过 IPv6 源
    if not KEEP_IPV6 and ("[" in url and "]" in url):
        return False

    async with semaphore:
        try:
            timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS, connect=CONNECT_TIMEOUT)
            # 先用 GET 请求读取首包（或 HEAD 请求）
            async with session.get(
                url, 
                headers=headers, 
                timeout=timeout, 
                allow_redirects=True,
                ssl=False
            ) as resp:
                if resp.status in [200, 206]:
                    chunk = await resp.content.read(256)
                    if chunk:
                        return True
        except Exception:
            pass
        return False

def parse_m3u(content):
    """解析 M3U 内容"""
    channels = []
    lines = content.splitlines()
    current_info = ""
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith("#EXTINF:"):
            current_info = line
        elif not line.startswith("#") and current_info:
            name_match = re.search(r',([^,]+)$', current_info)
            channel_name = name_match.group(1).strip() if name_match else "未知频道"
            channels.append({
                "info": current_info,
                "name": channel_name,
                "url": line
            })
            current_info = ""
    return channels

async def main():
    if not os.path.exists(SOURCES_FILE):
        print(f"错误: 找不到 {SOURCES_FILE}")
        return

    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        sources = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    all_channels = []
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_CHECKS, ssl=False)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        for src in sources:
            try:
                async with session.get(src, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        channels = parse_m3u(text)
                        all_channels.extend(channels)
            except Exception as e:
                print(f"读取上游失败 {src}: {e}")

        print(f"共采集到 {len(all_channels)} 个频道的直播流，开始验证稳定性...")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)
        valid_channels = []
        seen_urls = set()

        tasks = []
        for ch in all_channels:
            if ch["url"] in seen_urls:
                continue
            seen_urls.add(ch["url"])
            tasks.append((ch, check_stream(session, semaphore, ch["url"])))

        results = await asyncio.gather(*[t[1] for t in tasks])
        
        for (ch, _), is_valid in zip(tasks, results):
            if is_valid:
                valid_channels.append(ch)

    print(f"验证完成！最终筛选出 {len(valid_channels)} 个可用直播源。")

    os.makedirs("dist", exist_ok=True)
    
    with open(OUTPUT_M3U, "w", encoding="utf-8") as f:
        f.write(f'#EXTM3U x-tvg-url="{EPG_URL}"\n')
        for ch in valid_channels:
            f.write(f'{ch["info"]}\n{ch["url"]}\n')

    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        for ch in valid_channels:
            f.write(f'{ch["name"]},{ch["url"]}\n')

if __name__ == "__main__":
    asyncio.run(main())
