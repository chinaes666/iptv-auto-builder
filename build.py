import asyncio
import re
import os
import aiohttp

# 1. 基础配置
SOURCES_FILE = "sources.txt"
OUTPUT_M3U = "dist/live.m3u"
OUTPUT_TXT = "dist/live.txt"
EPG_URL = "http://epg.51zmt.top:8000/e.xml"

# 【防卡死关键配置】
TIMEOUT_SECONDS = 2.5       # 总体超时时间（秒），越短筛出的源越开得快
CONNECT_TIMEOUT = 1.5       # 建立 TCP 连接超时（秒），快速跳过打不通的服务器
MAX_CONCURRENT_CHECKS = 50  # 提高并发上限至 50

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

async def check_stream(session, semaphore, url):
    """验证单个直播源连通性及响应时间"""
    if not url.startswith(("http://", "https://")):
        return False

    async with semaphore:
        try:
            # 强行限定 connect 超时与 total 超时，跳过无效证书
            timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS, connect=CONNECT_TIMEOUT)
            async with session.get(
                url, 
                headers=headers, 
                timeout=timeout, 
                allow_redirects=True,
                ssl=False  # 忽略 SSL 证书错误，防止因证书问题卡死
            ) as resp:
                if resp.status in [200, 206]:
                    # 尝试读取前 512 字节数据，快速判断是否为有效流
                    chunk = await resp.content.read(512)
                    if chunk:
                        return True
        except Exception:
            pass
        return False

def parse_m3u(content):
    """解析 M3U 内容返回频道列表"""
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
    # 使用 custom TCPConnector 提升连接复用和 DNS 处理
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_CHECKS, ssl=False)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        for src in sources:
            try:
                # 给上游 M3U 下载也加上 8 秒硬超时
                async with session.get(src, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as resp:
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

        # 使用 asyncio.gather 替代逐个等待，提高调度效率
        results = await asyncio.gather(*[t[1] for t in tasks])
        
        for (ch, _), is_valid in zip(tasks, results):
            if is_valid:
                valid_channels.append(ch)

    print(f"验证完成！最终筛选出 {len(valid_channels)} 个高质量稳定直播源。")

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
