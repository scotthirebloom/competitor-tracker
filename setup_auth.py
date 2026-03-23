"""
One-time LinkedIn authentication setup.

Run this before the first use of the competitive tracker:
    python setup_auth.py

A visible browser window will open. Log in to LinkedIn manually.
Once you see your LinkedIn feed, come back here and press Enter.
Your session is saved to data/linkedin_session.json and reused on
every subsequent tracker run (sessions typically last 30-90 days).

When the session expires, the tracker will warn you in the Slack
digest — just run this script again.
"""
import asyncio
from pathlib import Path

from playwright.async_api import async_playwright

from tracker.file_io import ensure_private_dir, write_private_json

SESSION_PATH = Path(__file__).parent / "data" / "linkedin_session.json"


async def main() -> None:
    ensure_private_dir(SESSION_PATH.parent)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            locale="en-US",
        )
        page = await context.new_page()
        await page.goto("https://www.linkedin.com/login")

        print("\n" + "=" * 60)
        print("LinkedIn login window is open.")
        print("1. Log in with your LinkedIn credentials.")
        print("2. Complete any 2FA / verification steps.")
        print("3. Wait until you can see your LinkedIn feed.")
        print("4. Come back here and press Enter.")
        print("=" * 60 + "\n")
        input("Press Enter once you are logged in and see your feed: ")

        # Save full storage state (cookies + localStorage)
        storage = await context.storage_state()
        write_private_json(SESSION_PATH, storage)

        print(f"\nSession saved to: {SESSION_PATH}")
        print("You can now run the tracker: python run.py")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
