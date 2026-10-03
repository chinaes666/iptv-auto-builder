import asyncio
import re
import aiohttp

# 1. 基础配置
SOURCES_FILE = "sources.txt"
OUTPUT_M3U = "dist/live.m3u"
OUTPUT_TXT = "dist/live.txt"
EPG_URL = "http://epg.51zmt.top:8000/e.xml"
TIMEOUT_SECONDS = 3.5  # 超时时间（秒），超过即判定不稳定
MAX_CONCURRENT_CHECKS = 30  # 最大并发检测数

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

async def check_stream(session, semaphore, url):
    """验证单个直播源连通性及响应时间"""
    # 过滤明显的组播源（rtp://）或无效格式，GitHub Runner 无法检测内网组播
    if not url.startswith(("http://", "https://")):
        return False

    async with semaphore:
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS), allow_redirects=True) as resp:
                if resp.status in [200, 206]:
                    # 尝试读取前 1KB 内容，防止某些“假 200”网页
                    chunk = await resp.content.read(1024)
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
            # 提取频道名称
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
    # 读取上游地址
    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        sources = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    all_channels = []
    async with aiohttp.ClientSession() as session:
        # 下载所有上游源
        for src in sources:
            try:
                async with session.get(src, headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        channels = parse_m3u(text)
                        all_channels.extend(channels)
            except Exception as e:
                print(f"读取上游失败 {src}: {e}")

        print(f"共采集到 {len(all_channels)} 个频道的直播流，开始验证稳定性...")

        # 对采集到的流并发去重与验证
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)
        valid_channels = []
        seen_urls = set()

        tasks = []
        for ch in all_channels:
            if ch["url"] in seen_urls:
                continue
            seen_urls.add(ch["url"])
            tasks.append((ch, check_stream(session, semaphore, ch["url"])))

        # 批量执行连通性校验
        for ch, task in tasks:
            is_valid = await task
            if is_valid:
                valid_channels.append(ch)

    print(f"验证完成！最终筛选出 {len(valid_channels)} 个高质量稳定直播源。")

    # 生成 M3U 文件
    import os
    os.makedirs("dist", exist_ok=True)
    
    with open(OUTPUT_M3U, "w", encoding="utf-8") as f:
        f.write(f'#EXTM3U x-tvg-url="{EPG_URL}"\n')
        for ch in valid_channels:
            f.write(f'{ch["info"]}\n{ch["url"]}\n')

    # 生成 TXT 文件（兼容部分传统电视盒子）
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        for ch in valid_channels:
            f.write(f'{ch["name"]},{ch["url"]}\n')

if __name__ == "__main__":
    asyncio.run(main())
