"""
Script 1: Get Samco Session Cookie
This script visits the Samco bhav copy page and extracts the session cookie.
You need to copy this cookie and paste it into the downloader script.

Author: Quant Analyst
Date: 2025-10-01
"""

import requests
from datetime import datetime

def get_samco_cookie():
    """
    Visit Samco bhav copy page and extract the session cookie.
    
    Returns:
        str: The ci_session cookie value or None if failed
    """
    
    url = "https://www.samco.in/bhavcopy-nse-bse-mcx"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    
    try:
        print("="*80)
        print("Samco Session Cookie Extractor")
        print("="*80)
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Visiting Samco bhav copy page...")
        print(f"URL: {url}")
        
        session = requests.Session()
        response = session.get(url, headers=headers, timeout=10)
        
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Response Status: {response.status_code}")
        
        if response.status_code == 200:
            # Extract ci_session cookie
            cookies = session.cookies.get_dict()
            
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Cookies received:")
            for name, value in cookies.items():
                print(f"  {name}: {value}")
            
            if 'ci_session' in cookies:
                cookie_value = cookies['ci_session']
                
                print("\n" + "="*80)
                print("✓ SUCCESS! Cookie extracted successfully!")
                print("="*80)
                print(f"\nYour Session Cookie:")
                print(f"\n{cookie_value}\n")
                print("="*80)
                print("\nCOPY THE ABOVE COOKIE VALUE and paste it in the downloader script")
                print("when prompted, or update the SAMCO_SESSION_COOKIE variable in config.py")
                print("="*80)
                
                return cookie_value
            else:
                print("\n✗ ERROR: No ci_session cookie found!")
                print("Available cookies:", list(cookies.keys()))
                return None
        else:
            print(f"\n✗ ERROR: Failed to load page. Status code: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"\n✗ ERROR: Network error occurred: {str(e)}")
        return None
    except Exception as e:
        print(f"\n✗ ERROR: Unexpected error: {str(e)}")
        return None


if __name__ == "__main__":
    cookie = get_samco_cookie()
    
    if cookie:
        print("\n\n📋 Quick Instructions:")
        print("1. Copy the cookie value shown above")
        print("2. Open samco_bhav_downloader.py")
        print("3. Paste it when prompted OR update config.py")
        print("4. Run: python samco_bhav_downloader.py")
    else:
        print("\n\n⚠️ Could not extract cookie. Please:")
        print("1. Check your internet connection")
        print("2. Verify the Samco website is accessible")
        print("3. Try again in a few seconds")

