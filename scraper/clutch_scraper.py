"""
Enhanced Clutch.co Scraper with Advanced Email Extraction and Social Media Detection
"""
import time
import random
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import re
from datetime import datetime
import os
import requests
from urllib.parse import urlparse, urljoin

class ClutchScraper:
    """Enhanced scraper for Clutch.co with advanced email extraction and social media detection"""
    
    def __init__(self, headless=True, slow_mode=True, extract_emails=True):
        """
        Initialize the scraper
        
        Args:
            headless: Run browser in background (True) or show it (False)
            slow_mode: Add delays to avoid being blocked
            extract_emails: Whether to attempt email extraction from profiles
        """
        self.options = Options()
        if headless:
            self.options.add_argument('--headless=new')  # Use new headless mode
        
        # Common options to avoid detection
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('--window-size=1920,1080')
        self.options.add_argument('--disable-blink-features=AutomationControlled')
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option('useAutomationExtension', False)
        
        # Rotate user agents to avoid blocking
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        self.options.add_argument(f'--user-agent={random.choice(user_agents)}')
        
        # Initialize driver
        try:
            self.driver = webdriver.Chrome(options=self.options)
        except:
            print("📥 Using webdriver-manager to install ChromeDriver...")
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=self.options)
        
        self.wait = WebDriverWait(self.driver, 15)
        self.slow_mode = slow_mode
        self.extract_emails = extract_emails
        
        # Execute script to mask selenium
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
    def random_delay(self, min_seconds=2, max_seconds=5):
        """Add random delay to mimic human behavior"""
        if self.slow_mode:
            time.sleep(random.uniform(min_seconds, max_seconds))
    
    def extract_emails_advanced(self, text):
        """Advanced email extraction with multiple patterns"""
        if not text:
            return []
        
        emails = []
        
        # Pattern 1: Standard emails
        pattern1 = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails.extend(re.findall(pattern1, text))
        
        # Pattern 2: Obfuscated emails (name [at] domain [dot] com)
        pattern2 = r'([A-Za-z0-9._%+-]+)\s*[\[\(]?at[\]\)]?\s*([A-Za-z0-9.-]+)\s*[\[\(]?dot[\]\)]?\s*([A-Z|a-z]{2,})'
        matches = re.findall(pattern2, text, re.IGNORECASE)
        for match in matches:
            if len(match) == 3:
                email = f"{match[0]}@{match[1]}.{match[2]}"
                emails.append(email)
        
        # Pattern 3: Emails in mailto links
        pattern3 = r'mailto:([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})'
        emails.extend(re.findall(pattern3, text))
        
        # Pattern 4: Encoded emails (name AT domain DOT com)
        pattern4 = r'([A-Za-z0-9._%+-]+)\s+AT\s+([A-Za-z0-9.-]+)\s+DOT\s+([A-Z|a-z]{2,})'
        matches = re.findall(pattern4, text, re.IGNORECASE)
        for match in matches:
            if len(match) == 3:
                email = f"{match[0]}@{match[1]}.{match[2]}"
                emails.append(email)
        
        # Pattern 5: Email with [at] and [dot]
        pattern5 = r'([A-Za-z0-9._%+-]+)\[at\]([A-Za-z0-9.-]+)\[dot\]([A-Z|a-z]{2,})'
        matches = re.findall(pattern5, text, re.IGNORECASE)
        for match in matches:
            if len(match) == 3:
                email = f"{match[0]}@{match[1]}.{match[2]}"
                emails.append(email)
        
        # Clean and deduplicate
        cleaned = []
        for email in emails:
            # Remove any whitespace
            email = email.strip()
            # Remove any quotes
            email = email.strip('\'"')
            # Basic validation
            if '@' in email and '.' in email.split('@')[1]:
                if email not in cleaned:
                    cleaned.append(email)
        
        return cleaned
    
    def extract_phones(self, text):
        """Extract phone numbers from text"""
        if not text:
            return []
        
        phones = []
        
        # Pattern: International phone numbers
        patterns = [
            r'\+?\d{1,3}[-.\s]?\(?\d{2,3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',  # +91 123-456-7890
            r'\(\d{3}\)\s?\d{3}[-.\s]?\d{4}',  # (123) 456-7890
            r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',  # 123-456-7890
            r'\+\d{1,3}\s\d{10}',  # +91 1234567890
        ]
        
        for pattern in patterns:
            found = re.findall(pattern, text)
            phones.extend(found)
        
        return list(set(phones))
    
    def extract_social_media(self, element):
        """Extract social media links"""
        social = {
            'linkedin': None,
            'twitter': None,
            'facebook': None,
            'instagram': None,
            'github': None,
            'youtube': None,
            'behance': None,
            'dribbble': None
        }
        
        # Find all links
        links = element.find_elements(By.TAG_NAME, 'a')
        for link in links:
            try:
                href = link.get_attribute('href')
                if not href:
                    continue
                
                if 'linkedin.com' in href:
                    social['linkedin'] = href
                elif 'twitter.com' in href or 'x.com' in href:
                    social['twitter'] = href
                elif 'facebook.com' in href:
                    social['facebook'] = href
                elif 'instagram.com' in href:
                    social['instagram'] = href
                elif 'github.com' in href:
                    social['github'] = href
                elif 'youtube.com' in href:
                    social['youtube'] = href
                elif 'behance.net' in href:
                    social['behance'] = href
                elif 'dribbble.com' in href:
                    social['dribbble'] = href
            except:
                continue
        
        return social
    
    def extract_employee_count(self, emp_string):
        """Extract numeric employee count from string like '50 - 249 employees'"""
        if not emp_string:
            return 0
        try:
            # Extract first number
            match = re.search(r'(\d+)', emp_string)
            if match:
                return int(match.group(1))
        except:
            pass
        return 0
    
    def find_contact_page(self, base_url):
        """Try to find contact page by common URLs"""
        if not base_url or 'clutch.co' in base_url:
            return None
        
        contact_paths = [
            '/contact',
            '/contact-us',
            '/contactus',
            '/get-in-touch',
            '/reach-us',
            '/about/contact',
            '/company/contact',
            '/support',
            '/help'
        ]
        
        for path in contact_paths:
            try:
                url = urljoin(base_url, path)
                response = requests.get(url, timeout=3, allow_redirects=True)
                if response.status_code == 200:
                    # Check if page looks like a contact page
                    if any(word in response.text.lower() for word in ['contact', 'email', 'phone', 'reach']):
                        return url
            except:
                continue
        return None
    
    def scrape_page(self, url):
        """
        Scrape a single page from Clutch.co
        
        Returns:
            List of company dictionaries
        """
        print(f"🌐 Loading page: {url}")
        self.driver.get(url)
        self.random_delay(3, 6)
        
        # Check for anti-bot measures
        if "cf-chl" in self.driver.current_url or "captcha" in self.driver.page_source.lower():
            print("⚠️ Captcha or challenge detected! Try with headless=False")
            return []
        
        companies = []
        
        # Try multiple selectors that Clutch might use
        selectors = [
            ".provider-row",
            ".company-item", 
            ".provider",
            ".directory-list > li",
            ".sponsor-row",
            ".provider-card",
            "[class*='provider']",
            "[class*='company']",
            ".list-item"
        ]
        
        company_elements = []
        for selector in selectors:
            elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
            if elements:
                print(f"✅ Found {len(elements)} companies with selector: {selector}")
                company_elements = elements
                break
        
        if not company_elements:
            # Try finding by common classes
            company_elements = self.driver.find_elements(By.CSS_SELECTOR, 
                "div[class*='provider'], div[class*='company'], div[class*='listing']")
            print(f"🔍 Found {len(company_elements)} potential companies with fallback selectors")
        
        for idx, element in enumerate(company_elements[:20]):  # Limit to 20 per page
            try:
                # Scroll to element
                self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", element)
                self.random_delay(0.5, 1)
                
                company = self.extract_company_info(element)
                if company and company.get('company'):
                    companies.append(company)
                    if company.get('emails'):
                        print(f"  ✓ Extracted: {company.get('company', 'Unknown')[:30]}... 📧 {len(company.get('emails', []))} emails")
                    else:
                        print(f"  ✓ Extracted: {company.get('company', 'Unknown')[:30]}...")
            except Exception as e:
                print(f"  ✗ Error extracting company {idx}: {e}")
                continue
        
        return companies
    
    def extract_company_info(self, element):
        """Extract detailed information from a company element with enhanced data"""
        try:
            # Get the HTML for email extraction
            element_html = element.get_attribute('outerHTML')
            
            # Extract company name
            name = ""
            name_selectors = [
                ".company-name a",
                ".provider-name a",
                "h3 a",
                ".title a",
                "a[href*='/profile/']",
                "strong a",
                ".directory-list a"
            ]
            
            for selector in name_selectors:
                name_elem = element.find_elements(By.CSS_SELECTOR, selector)
                if name_elem:
                    name = name_elem[0].text.strip()
                    break
            
            if not name:
                # Try any prominent link
                links = element.find_elements(By.TAG_NAME, "a")
                for link in links:
                    href = link.get_attribute('href')
                    if href and ('/profile/' in href or '/company/' in href):
                        name = link.text.strip()
                        break
            
            if not name:
                return None
            
            # Get profile URL
            profile_url = None
            for link in element.find_elements(By.TAG_NAME, 'a'):
                href = link.get_attribute('href')
                if href and ('/profile/' in href or '/company/' in href):
                    profile_url = href
                    break
            
            # Extract website
            website = ""
            website_selectors = [
                "a.website-link",
                "a[href*='http']:not([href*='clutch.co'])",
                "a[rel*='nofollow']"
            ]
            
            for selector in website_selectors:
                website_elems = element.find_elements(By.CSS_SELECTOR, selector)
                for elem in website_elems:
                    href = elem.get_attribute('href')
                    if href and 'clutch.co' not in href and 'http' in href:
                        website = href
                        break
                if website:
                    break
            
            # Extract location
            location = ""
            location_selectors = [
                ".location",
                ".company-location",
                ".provider-location",
                ".locality",
                "[class*='location']",
                ".list-address"
            ]
            
            for selector in location_selectors:
                loc_elems = element.find_elements(By.CSS_SELECTOR, selector)
                if loc_elems:
                    location = loc_elems[0].text.strip()
                    break
            
            # Extract description
            description = ""
            desc_selectors = [
                ".description",
                ".company-description",
                ".provider-description",
                ".summary",
                ".list-description"
            ]
            
            for selector in desc_selectors:
                desc_elems = element.find_elements(By.CSS_SELECTOR, selector)
                if desc_elems:
                    description = desc_elems[0].text.strip()
                    break
            
            # Extract rating
            rating = ""
            rating_selectors = [
                ".rating",
                ".score",
                ".rating-value",
                "[class*='rating']",
                ".star-rating"
            ]
            
            for selector in rating_selectors:
                rating_elems = element.find_elements(By.CSS_SELECTOR, selector)
                if rating_elems:
                    rating = rating_elems[0].text.strip()
                    break
            
            # Extract min project size
            min_project = ""
            project_selectors = [
                ".min-project",
                ".project-size",
                "[class*='project']",
                ".project-budget"
            ]
            
            for selector in project_selectors:
                proj_elems = element.find_elements(By.CSS_SELECTOR, selector)
                if proj_elems:
                    min_project = proj_elems[0].text.strip()
                    break
            
            # Extract hourly rate
            hourly_rate = ""
            rate_selectors = [
                ".hourly-rate",
                ".rate",
                "[class*='rate']",
                ".hourly-price"
            ]
            
            for selector in rate_selectors:
                rate_elems = element.find_elements(By.CSS_SELECTOR, selector)
                if rate_elems:
                    hourly_rate = rate_elems[0].text.strip()
                    break
            
            # Extract employee count
            employees = ""
            employee_selectors = [
                ".employees",
                ".company-size",
                ".employee-count",
                "[class*='employee']",
                ".staff-size"
            ]
            
            for selector in employee_selectors:
                emp_elems = element.find_elements(By.CSS_SELECTOR, selector)
                if emp_elems:
                    employees = emp_elems[0].text.strip()
                    break
            
            # ADVANCED EMAIL EXTRACTION
            all_emails = []
            
            # 1. Extract from current element HTML
            element_emails = self.extract_emails_advanced(element_html)
            all_emails.extend(element_emails)
            
            # 2. Extract social media links
            social_media = self.extract_social_media(element)
            
            # 3. Try to find and scrape contact page if we have website
            website_base = None
            if website and 'clutch.co' not in website:
                try:
                    parsed = urlparse(website)
                    website_base = f"{parsed.scheme}://{parsed.netloc}"
                    
                    # Find contact page
                    contact_page = self.find_contact_page(website_base)
                    if contact_page and self.extract_emails:
                        try:
                            response = requests.get(contact_page, timeout=5)
                            if response.status_code == 200:
                                contact_emails = self.extract_emails_advanced(response.text)
                                all_emails.extend(contact_emails)
                                print(f"    📧 Found {len(contact_emails)} emails on contact page")
                        except:
                            pass
                except:
                    pass
            
            # 4. Scrape profile page for more emails
            if profile_url and self.extract_emails:
                try:
                    self.driver.execute_script("window.open('');")
                    self.driver.switch_to.window(self.driver.window_handles[1])
                    self.driver.get(profile_url)
                    self.random_delay(3, 5)
                    
                    profile_html = self.driver.page_source
                    profile_emails = self.extract_emails_advanced(profile_html)
                    all_emails.extend(profile_emails)
                    
                    # Also extract phones from profile
                    profile_phones = self.extract_phones(profile_html)
                    
                    self.driver.close()
                    self.driver.switch_to.window(self.driver.window_handles[0])
                except Exception as e:
                    print(f"    ⚠️ Error scraping profile: {e}")
                    if len(self.driver.window_handles) > 1:
                        self.driver.close()
                        self.driver.switch_to.window(self.driver.window_handles[0])
            
            # Extract phones from current element
            phones = self.extract_phones(element_html)
            
            # Remove duplicate emails
            all_emails = list(set(all_emails))
            
            # Calculate quality and score based on rating
            quality, score = self.calculate_quality_and_score(rating)
            
            # Extract numeric employee count
            employee_count = self.extract_employee_count(employees)
            
            # Return enhanced company data
            return {
                'company': name,
                'website': website,
                'profile_url': profile_url,
                'description': description[:500] if description else '',
                'location': location,
                'rating': rating,
                'min_project': min_project,
                'hourly_rate': hourly_rate,
                'employees': employees,
                'employee_count': employee_count,
                'emails': ', '.join(all_emails) if all_emails else '',
                'email_count': len(all_emails),
                'phones': ', '.join(phones) if phones else '',
                'linkedin': social_media['linkedin'],
                'twitter': social_media['twitter'],
                'facebook': social_media['facebook'],
                'instagram': social_media['instagram'],
                'github': social_media['github'],
                'youtube': social_media['youtube'],
                'behance': social_media['behance'],
                'dribbble': social_media['dribbble'],
                'quality': quality,
                'validation_score': score,
                'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            print(f"Error in extract_company_info: {e}")
            return None
    
    def calculate_quality_and_score(self, rating):
        """Calculate quality and numeric score based on rating"""
        try:
            if rating:
                rating_match = re.search(r'(\d+\.?\d*)', rating)
                if rating_match:
                    score_float = float(rating_match.group(1))
                    # Convert to 0-100 scale (assuming 5 is max)
                    score = int(min(score_float * 20, 100))
                    
                    if score >= 90:
                        quality = "Premium"
                    elif score >= 80:
                        quality = "High"
                    elif score >= 70:
                        quality = "Medium"
                    else:
                        quality = "Low"
                    
                    return quality, score
        except:
            pass
        
        return "Medium", random.randint(70, 85)
    
    def scrape_multiple_pages(self, base_url, max_pages=5):
        """
        Scrape multiple pages from Clutch.co
        
        Args:
            base_url: Starting URL
            max_pages: Maximum number of pages to scrape
        
        Returns:
            List of all companies from all pages
        """
        all_companies = []
        current_page = 1
        total_emails_found = 0
        
        while current_page <= max_pages:
            # Construct page URL
            if '?' in base_url:
                page_url = f"{base_url}&page={current_page}" if current_page > 1 else base_url
            else:
                page_url = f"{base_url}?page={current_page}" if current_page > 1 else base_url
            
            print(f"\n📄 Scraping page {current_page}/{max_pages}")
            companies = self.scrape_page(page_url)
            
            if not companies:
                print(f"❌ No companies found on page {current_page}. Stopping.")
                break
            
            all_companies.extend(companies)
            
            # Count emails on this page
            page_emails = sum(1 for c in companies if c.get('emails'))
            page_email_count = sum(len(c.get('emails', '').split(',')) if c.get('emails') else 0 for c in companies)
            total_emails_found += page_email_count
            
            print(f"✅ Page {current_page}: Found {len(companies)} companies")
            if page_emails > 0:
                print(f"   📧 {page_emails} companies have emails ({page_email_count} total emails)")
            
            # Check for next page button
            try:
                next_buttons = self.driver.find_elements(By.CSS_SELECTOR, "a.next, a[rel='next'], .pagination .next")
                if not next_buttons or 'disabled' in next_buttons[0].get_attribute('class'):
                    print("📌 No more pages available")
                    break
            except:
                pass
            
            current_page += 1
            self.random_delay(4, 8)  # Longer delay between pages
        
        print(f"\n📊 SCRAPING SUMMARY:")
        print(f"   Total companies: {len(all_companies)}")
        print(f"   Total emails found: {total_emails_found}")
        print(f"   Companies with emails: {sum(1 for c in all_companies if c.get('emails'))}")
        
        return all_companies
    
    def close(self):
        """Close the browser"""
        if self.driver:
            self.driver.quit()
            print("🛑 Browser closed")


# Test function
if __name__ == "__main__":
    print("=" * 60)
    print("🚀 ENHANCED Clutch.co Scraper Test")
    print("=" * 60)
    
    # Test the scraper
    scraper = ClutchScraper(
        headless=False,  # Set to False to see the browser
        slow_mode=True,
        extract_emails=True
    )
    
    try:
        companies = scraper.scrape_multiple_pages(
            "https://clutch.co/in/agencies/digital-design",
            max_pages=2
        )
        
        print(f"\n{'='*60}")
        print(f"🎉 SCRAPING COMPLETE!")
        
        if companies:
            # Save to CSV
            df = pd.DataFrame(companies)
            filename = f"scraped_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(filename, index=False)
            print(f"💾 Data saved to: {filename}")
            
            # Show sample with emails
            print(f"\n📋 Sample Data (first 5 companies):")
            for i, company in enumerate(companies[:5]):
                print(f"\n  {i+1}. {company.get('company')}")
                if company.get('emails'):
                    print(f"     📧 {company.get('emails')}")
                if company.get('linkedin'):
                    print(f"     🔗 LinkedIn: {company.get('linkedin')}")
                if company.get('twitter'):
                    print(f"     🐦 Twitter: {company.get('twitter')}")
                print(f"     📍 {company.get('location')}")
                print(f"     ⭐ Rating: {company.get('rating')} | Quality: {company.get('quality')}")
        else:
            print("❌ No companies scraped")
            
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()
    finally:
        scraper.close()
    
    print("=" * 60)