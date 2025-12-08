"""
Final working scraper for pump.fun memecoins
Extracts addresses, names, and symbols from the table view
"""

import asyncio
import csv
import re
from pathlib import Path
from typing import List, Dict
import argparse


async def scrape_page_content(page, debug: bool = False) -> List[Dict[str, str]]:
    """
    Scrape the current page content (assumes page is already loaded)
    """
    # Wait for table
    print("Waiting for table to load...")
    await page.wait_for_selector('table tbody tr', state="attached", timeout=30000)

    # Wait for actual content to load - look for cursor-pointer class which indicates real rows
    print("Waiting for content to finish loading...")
    max_attempts = 10
    for attempt in range(max_attempts):
        # Check if we have real content (rows with cursor-pointer class)
        real_rows = await page.query_selector_all('table tbody tr.cursor-pointer')
        if len(real_rows) > 0:
            print(f"Found {len(real_rows)} loaded rows")
            break

        if attempt < max_attempts - 1:
            await asyncio.sleep(2)
        else:
            print("Warning: No content rows found, continuing anyway...")

    # Scroll to trigger lazy loading
    await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
    await asyncio.sleep(2)
    await page.evaluate('window.scrollTo(0, 0)')
    await asyncio.sleep(2)

    if debug:
        await page.screenshot(path="debug_screenshot.png", full_page=True)
        print("Screenshot saved")

    # Get table rows
    print("Extracting coin data...")
    rows = await page.query_selector_all('table tbody tr')

    if debug:
        print(f"Found {len(rows)} table rows")

    coins = []
    seen_addresses = set()

    for idx, row in enumerate(rows):
        try:
            # Get row HTML to search for address
            row_html = await row.evaluate('el => el.outerHTML')
            row_text = await row.inner_text()

            if debug and idx == 0:
                print(f"\nFirst row HTML (truncated): {row_html[:500]}...")
                print(f"First row text: {row_text[:200]}...")

            # Find address in the HTML - look for pump addresses (end with 'pump')
            address_match = re.search(r'([A-Za-z0-9]{30,50}pump)', row_html)
            if not address_match:
                # Try any Solana address pattern
                address_match = re.search(r'/coin/([A-Za-z0-9]{30,50})', row_html)

            # Try looking for it in onclick or other attributes
            if not address_match:
                address_match = re.search(r'([A-Za-z0-9]{40,50})', row_html)

            if not address_match:
                if debug and idx < 3:
                    print(f"  Row {idx+1}: No address found in HTML")
                continue

            address = address_match.group(1) if address_match.lastindex is None else address_match.group(1)

            # Skip duplicates
            if address in seen_addresses:
                continue
            seen_addresses.add(address)

            # Parse name and symbol from row text
            lines = [line.strip() for line in row_text.split('\n') if line.strip()]

            name = ""
            symbol = ""

            # First line usually contains: #1Name Symbol or #1NameSymbol or just coin info
            # Let's parse the first line more carefully
            if lines:
                first_line = lines[0]

                # Remove rank number like #1, #2, etc
                first_line = re.sub(r'^#\d+', '', first_line).strip()

                # Now we might have something like "Fartcoin Fartcoin $224..." or "Alchemist AIALCH$140..."
                # Split by $ to remove price info
                if '$' in first_line:
                    first_line = first_line.split('$')[0].strip()

                # Now split into words
                words = first_line.split()

                if len(words) >= 2:
                    # Common pattern: "Name SYMBOL" or "Name Name" or "Multi Word Name SYMBOL"
                    # Last word is often the symbol if it's short and uppercase-heavy
                    last_word = words[-1]
                    if len(last_word) <= 10 and sum(c.isupper() for c in last_word) >= len(last_word) * 0.5:
                        # Likely a symbol
                        symbol = last_word
                        name = ' '.join(words[:-1])
                    else:
                        # Might be repeated name or multi-word name
                        name = ' '.join(words)
                elif len(words) == 1:
                    name = words[0]

            coins.append({
                'address': address,
                'name': name,
                'symbol': symbol,
                'url': f'https://pump.fun/coin/{address}'
            })

            if debug and len(coins) <= 5:
                print(f"  Coin {len(coins)}: {name} ({symbol}) - {address[:20]}...")

        except Exception as e:
            if debug:
                print(f"Error processing row {idx+1}: {e}")
            continue

    print(f"Extracted {len(coins)} coins from this page")
    return coins


async def scrape_pump_fun(base_url: str, max_pages: int = 1, debug: bool = False) -> List[Dict[str, str]]:
    """
    Scrape pump.fun using Playwright, supporting multiple pages
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        raise ImportError("Playwright not installed. Run: uv add playwright && uv run playwright install chromium")

    print("Using Playwright to scrape...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not debug)
        page = await browser.new_page()

        all_coins = []
        seen_addresses = set()

        # Check if there's a search query
        has_search = '&q=' in base_url or '?q=' in base_url

        for page_num in range(max_pages):
            print(f"\n--- Page {page_num + 1}/{max_pages} ---")

            # Only navigate on first page, or if no search (can use URL offset)
            if page_num == 0:
                print(f"Navigating to {base_url}...")
                await page.goto(base_url, wait_until="domcontentloaded", timeout=60000)
            elif not has_search:
                # No search - can use URL offset
                offset = page_num * 48
                separator = '&' if '?' in base_url else '?'
                url = f"{base_url}{separator}offset={offset}"
                print(f"Navigating to {url}...")
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            else:
                # Has search - use pagination controls instead
                print("Clicking next page...")
                try:
                    # Look for "next" or ">>" button
                    next_button = await page.wait_for_selector('button:has-text(">>"), a:has-text(">>"), [class*="next"]', timeout=5000)
                    await next_button.click()
                    await asyncio.sleep(3)
                except Exception as e:
                    print(f"Could not find next page button: {e}")
                    break

            # Only dismiss popups and handle search on first page (AFTER navigation)
            if page_num == 0:
                # Dismiss popups
                print("Checking for popups...")
                try:
                    pump_button = await page.wait_for_selector('button:has-text("I\'m ready to pump")', timeout=5000)
                    if pump_button:
                        print("Dismissing popup...")
                        await pump_button.click()
                        await asyncio.sleep(1)
                except:
                    pass

                try:
                    skip_button = await page.wait_for_selector('button:has-text("Skip")', timeout=5000)
                    if skip_button:
                        print("Dismissing mayhem popup...")
                        await skip_button.click()
                        await asyncio.sleep(1)
                except:
                    pass

                # Accept cookies if present
                try:
                    accept_cookies = await page.wait_for_selector('button:has-text("Accept All")', timeout=5000)
                    if accept_cookies:
                        print("Accepting cookies...")
                        await accept_cookies.click()
                        await asyncio.sleep(1)
                except:
                    pass

                # If there's a query, enter it in the search box
                if '&q=' in base_url or '?q=' in base_url:
                    # Extract the query from URL
                    import urllib.parse
                    parsed = urllib.parse.urlparse(base_url)
                    params = urllib.parse.parse_qs(parsed.query)
                    query = params.get('q', [''])[0]

                    if query:
                        print(f"Entering search query: {query}")
                        try:
                            # Find and fill the search input
                            search_input = await page.wait_for_selector('input[placeholder*="Search"]', timeout=5000)
                            await search_input.fill(query)
                            await asyncio.sleep(0.5)

                            # Click the Search button
                            search_button = await page.wait_for_selector('button:has-text("Search")', timeout=5000)
                            await search_button.click()
                            print("Search submitted, waiting for results...")
                            await asyncio.sleep(3)
                        except Exception as e:
                            print(f"Error performing search: {e}")

            # Scrape the page (navigation already done above)
            page_coins = await scrape_page_content(page, debug)

            # Filter out duplicates across pages
            new_coins = []
            for coin in page_coins:
                if coin['address'] not in seen_addresses:
                    seen_addresses.add(coin['address'])
                    new_coins.append(coin)
                    all_coins.append(coin)

            print(f"Added {len(new_coins)} new coins (total: {len(all_coins)})")

            # If we got no new coins, we've probably reached the end
            if len(new_coins) == 0:
                print("No new coins found, stopping pagination")
                break

            # Small delay between pages to be respectful
            if page_num < max_pages - 1:
                await asyncio.sleep(2)

        await browser.close()

        print(f"\n=== Total: Extracted {len(all_coins)} unique coins across {page_num + 1} pages ===")
        return all_coins


def save_to_csv(coins: List[Dict[str, str]], output_file: str):
    """Save coins to CSV file"""
    if not coins:
        print("No coins to save!")
        return

    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['address', 'name', 'symbol', 'url']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(coins)

    print(f"Saved to {output_file}")


async def main():
    parser = argparse.ArgumentParser(description='Scrape memecoins from pump.fun')
    parser.add_argument('--query', '-q', default='', help='Search query')
    parser.add_argument('--sort', '-s', default='market_cap', help='Sort by (default: market_cap)')
    parser.add_argument('--pages', '-p', type=int, default=1, help='Number of pages to scrape (default: 1, each page = ~48 coins)')
    parser.add_argument('--output', '-o', default='', help='Output file (default: data/coin_search/<query>.csv)')
    parser.add_argument('--debug', '-d', action='store_true', help='Debug mode')

    args = parser.parse_args()

    # Determine output filename
    if args.output:
        output_file = args.output
    elif args.query:
        # Use query as filename, sanitize it
        import re
        safe_query = re.sub(r'[^\w\s-]', '', args.query).strip().replace(' ', '_')
        os.makedirs("data/coin_search/", exist_ok=True)
        output_file = f"data/coin_search/{safe_query}.csv"
    else:
        output_file = "data/coin_search/pump_coins.csv"

    # Build base URL (without offset - that's handled by scrape_pump_fun)
    url = f"https://pump.fun/?sort={args.sort}&view=table"
    if args.query:
        url += f"&q={args.query.replace(' ', '+')}"

    print(f"Scraping: {url}")
    print(f"Pages to scrape: {args.pages} (~{args.pages * 48} coins max)")
    print(f"Output file: {output_file}\n")

    try:
        coins = await scrape_pump_fun(url, max_pages=args.pages, debug=args.debug)

        if not coins:
            print("\nNo coins found!")
            return

        # Save
        save_to_csv(coins, output_file)

        # Print sample
        print("\nSample coins:")
        for coin in coins[:10]:
            print(f"  {coin['name']} ({coin['symbol']}) - {coin['address']}")

    except Exception as e:
        print(f"Error: {e}")
        if not args.debug:
            print("Try running with --debug flag")


if __name__ == '__main__':
    asyncio.run(main())
