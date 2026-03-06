from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict
import pandas as pd
import glob
import os
from datetime import datetime
import json
import time
import random
import traceback
import re

app = FastAPI(title="Clutch.co Lead Finder API FIXED", version="1.0.0")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8000", "http://127.0.0.1:8000", "http://localhost:8001", "http://127.0.0.1:8001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Ensure data directory exists
os.makedirs("data", exist_ok=True)

class ScrapeRequest(BaseModel):
    category: str
    pages: int = 10

def get_latest_file(pattern="*.csv"):
    """Get the most recent CSV file"""
    try:
        files = glob.glob(f"data/{pattern}")
        if not files:
            return None
        return max(files, key=os.path.getctime)
    except Exception as e:
        print(f"Error in get_latest_file: {e}")
        return None

def extract_min_employee_count(emp_string):
    """Extract minimum employee count from string like '50-249 employees'"""
    if not emp_string or pd.isna(emp_string):
        return 0
    try:
        emp_str = str(emp_string)
        # Find the first number in the string
        match = re.search(r'(\d+)', emp_str)
        if match:
            return int(match.group(1))
    except:
        pass
    return 0

def generate_mock_leads(category, count=30):
    """Generate mock lead data for testing"""
    
    # Category-specific company names
    category_companies = {
        "SEO Agencies": [
            "RankBoost", "SearchGenius", "TrafficMax", "SEO Pros", "KeywordKings",
            "LinkBuilders", "PageRankers", "SearchLabs", "OrganicMasters", "WebSEO"
        ],
        "Logo Design Agencies": [
            "LogoMaster", "Iconic Designs", "BrandMark", "PixelLogo", "DesignCrest",
            "CreativeMarks", "LogoLab", "BrandCraft", "IdentityPro", "VisualMarks"
        ],
        "Web Design Companies": [
            "WebWizards", "SiteCraft", "PagePioneers", "UX Masters", "DesignStudio",
            "PixelPerfect", "CreativeWeb", "DesignHub", "WebArtisans", "CodeCraft"
        ],
        "Digital Marketing Agencies": [
            "MarketSmart", "GrowthHackers", "SocialBlast", "AdVantage", "ClickGenius",
            "DigitalRise", "SocialPros", "AdMasters", "ConversionPros", "MarketingLab"
        ],
        "Software Developers": [
            "CodeMasters", "DevForce", "AppWorks", "TechSolutions", "InnovateLabs",
            "SoftCore", "DevStudio", "AppGenius", "CodeCrafters", "TechPro"
        ],
        "Mobile Developers": [
            "MobileMasters", "AppCraft", "iOS Pros", "Android Wizards", "MobileFirst",
            "AppStudio", "MobileGenius", "AppWorks", "MobileLab", "CodeMobile"
        ],
        "AI Developers": [
            "AILabs", "SmartSystems", "NeuralWorks", "DeepLearn", "AI Solutions",
            "CogniTech", "MindLabs", "DataScience Pro", "AI Masters", "IntelliSoft"
        ],
        "Cloud Consulting": [
            "CloudMasters", "AWS Pros", "Azure Experts", "CloudSolutions", "DevOps Pro",
            "CloudNative", "Kubernetes Labs", "CloudArchitects", "CloudOps", "DataCloud"
        ],
        "Digital Design Agencies in India": [
            "Solid Digital", "ControlF5", "Yellow Slice", "Codal", "SparxIT",
            "PageTraffic", "divami", "GeekyAnts", "Konstant Infosolutions", "Digital Aptech",
            "Aalpha Information Systems", "JPLoft", "Skynet Technologies", "Elsner Technologies", 
            "Infowind Technologies", "Matrix Media Solutions", "LN Webworks", "Flowtrix", 
            "Unified Infotech", "Reckonsys"
        ],
        "Web Design Companies in India": [
            "Webchutney", "Eleven Digital", "Fractal Ink", "BC Web Wise", "Digital Refresh",
            "Pixelette Technologies", "Pixelperfx", "Alphanso Pixel", "TechShu", "Brainpulse"
        ],
        "Mobile Developers in India": [
            "Hidden Brains", "TechAhead", "OpenXcell", "Space-O Technologies", "Xicom Technologies",
            "Savvy Apps", "Zco Corporation", "Y Media Labs", "WillowTree", "Appster"
        ],
        "SEO Agencies in India": [
            "PageTraffic", "ThatWare", "Techmagnate", "SEOValley", "Ebrandz",
            "iProspect", "Webpulse", "Moz", "SEMRush", "SearchTrade"
        ],
        "Software Developers in India": [
            "Tata Consultancy Services", "Infosys", "Wipro", "HCL Technologies", "Tech Mahindra",
            "Mindtree", "L&T Infotech", "Mphasis", "Hexaware", "Persistent Systems"
        ],
        "Cloud Consulting in India": [
            "Cloud4C", "Netmagic Solutions", "Sify Technologies", "Tata Communications", "CtrlS",
            "ESDS Software", "NxtGen", "Yotta Infrastructure", "RackBank", "Web Werks"
        ],
        # 🌟 NEW CATEGORIES
        "Blockchain Developers in India": [
            "Primafelicitas", "TechBeezer", "Blockchain India", "Koinos", "CoinCrowd",
            "Blockchained India", "Wish Airdrop", "Block Gemini", "Sofocle", "Minddeft"
        ],
        "Game Development Companies in India": [
            "Ubisoft India", "Rockstar Games India", "Dhruva Interactive", "99Games", "Hashstash",
            "Moonfrog Labs", "Nazara Games", "Octro", "Hungama Games", "Truly Madly Games"
        ],
        "E-commerce Developers in India": [
            "Flipkart Tech", "Amazon India Dev", "Shopify Experts India", "Magento India", "Woocommerce India",
            "Cart2Cart", "KartRocket", "ShopClues Tech", "Paytm Mall Tech", "Snapdeal Tech"
        ],
        "Digital Strategy Agencies in India": [
            "WATConsult", "Social Panga", "Digital Refresh", "BC Web Wise", "Schbang",
            "Kinnect", "FoxyMoron", "Gozoop", "Social Beat", "L&K Saatchi & Saatchi"
        ],
        "Video Production Companies in India": [
            "TVC India", "Viral Films", "Content Sutra", "Video Wizards", "Reel India",
            "MotionPixel", "Dream Merchants", "Pocket Films", "Chimney Laundry", "Early Man Film"
        ],
        "Content Marketing Agencies in India": [
            "Contently India", "Viral Content", "Content Ninja", "WriteRight", "CopyLove",
            "Content Sutra", "StoryBites", "WordSmith", "ContentWorks", "BlogMasters India"
        ],
        "Branding Agencies in India": [
            "Landor & Fitch India", "Interbrand India", "Siegel+Gale India", "Wolff Olins India",
            "FutureBrand India", "BrandUnion", "Talking Point", "BrandCare", "Hue & Cry", "Trizone"
        ],
        "PPC Agencies in India": [
            "AdLift", "iProspect India", "SearchTrade", "ThatWare", "Webchutney",
            "Digital Refresh Networks", "Pinstorm", "Gozoop", "WATConsult", "Social Beat"
        ],
        "Social Media Marketing in India": [
            "Social Beat", "Gozoop", "WATConsult", "Social Panga", "Digital Refresh Networks",
            "L&K Saatchi & Saatchi", "Schbang", "Kinnect", "BC Web Wise", "FoxyMoron"
        ],
        "UI/UX Design Agencies in India": [
            "Fractal Ink", "Mutual Mobile", "Exilant", "UXReactor", "RighTpoint",
            "ZEUX Innovation", "User Experience Design", "Flytbase", "UXD India", "Crafted"
        ],
    }
    
    # Default companies if category not found
    default_companies = [
        "TechCorp", "InnovateInc", "DigitalDynamics", "CreativeSolutions", "ProServices",
        "NextGen Tech", "Prime Solutions", "Elite Services", "Peak Performance", "Summit Digital"
    ]
    
    # Get companies for this category or use default
    companies = category_companies.get(category, default_companies)
    
    # India-specific locations for India categories
    india_locations = [
        "Mumbai, India", "Delhi, India", "Bangalore, India", "Hyderabad, India",
        "Chennai, India", "Kolkata, India", "Pune, India", "Ahmedabad, India",
        "Jaipur, India", "Lucknow, India", "Indore, India", "Nagpur, India",
        "Surat, India", "Visakhapatnam, India", "Bhopal, India", "Patna, India"
    ]
    
    # US locations for other categories
    us_locations = [
        "San Francisco, CA", "New York, NY", "Austin, TX", "Boston, MA", 
        "Chicago, IL", "Seattle, WA", "Los Angeles, CA", "Denver, CO",
        "Miami, FL", "Portland, OR", "Atlanta, GA", "Dallas, TX",
        "San Diego, CA", "Philadelphia, PA", "Phoenix, AZ", "Detroit, MI"
    ]
    
    # Choose location set based on category
    if "India" in category:
        locations = india_locations
    else:
        locations = us_locations
    
    leads = []
    for i in range(min(count, 30)):
        # Cycle through companies
        company_name = companies[i % len(companies)]
        if i >= len(companies):
            company_name = f"{company_name} {i+1}"
        
        # Generate random score with more variety for better chart display
        score = random.randint(65, 98)
        
        # Determine quality based on score
        if score >= 90:
            quality = "Premium"
        elif score >= 80:
            quality = "High"
        elif score >= 70:
            quality = "Medium"
        else:
            quality = "Low"
        
        # Generate rating based on score
        rating_value = score / 20
        rating = f"{rating_value:.1f} ({random.randint(10, 500)} reviews)"
        
        # Generate employee count with proper ranges INCLUDING SMALL COMPANIES
        employee_ranges = ["1-9", "10-49", "50-249", "250-999", "1000-5000", "5000+"]
        employee_range = random.choice(employee_ranges)
        
        # Extract numeric value for filtering
        if employee_range == "1-9":
            emp_count = random.randint(1, 9)
        elif employee_range == "10-49":
            emp_count = random.randint(10, 49)
        elif employee_range == "50-249":
            emp_count = random.randint(50, 249)
        elif employee_range == "250-999":
            emp_count = random.randint(250, 999)
        elif employee_range == "1000-5000":
            emp_count = random.randint(1000, 5000)
        else:  # "5000+"
            emp_count = random.randint(5001, 10000)
        
        lead = {
            "company": company_name,
            "website": f"https://www.{company_name.lower().replace(' ', '').replace(',', '')}.com",
            "quality": quality,
            "validation_score": score,
            "category": category,
            "location": random.choice(locations),
            "rating": rating,
            "description": f"Leading provider of {category.lower()} services with over {random.randint(5, 25)} years of experience.",
            "employees": f"{employee_range} employees",  # Display text
            "employee_count": emp_count,  # Numeric for filtering
            "email": f"info@{company_name.lower().replace(' ', '').replace(',', '')}.com",
            "phone": f"+91 {random.randint(7000000000, 9999999999)}",
            "linkedin": f"https://linkedin.com/company/{company_name.lower().replace(' ', '')}",
            "twitter": f"https://twitter.com/{company_name.lower().replace(' ', '')}",
            "scraped_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        leads.append(lead)
    
    return leads

# === COMPLETE URL DICTIONARY WITH ALL CATEGORIES ===
urls = {
    # Original Tech URLs
    "developers": "https://clutch.co/developers",
    "mobile": "https://clutch.co/developers/mobile",
    "web": "https://clutch.co/developers/web",
    "ai": "https://clutch.co/developers/ai",
    "cloud-consulting": "https://clutch.co/cloud-consulting",
    "it-services": "https://clutch.co/it-services",
    "python": "https://clutch.co/developers/python",
    "javascript": "https://clutch.co/developers/javascript",
    "react": "https://clutch.co/developers/react",
    "devops": "https://clutch.co/devops",
    
    # India-specific URLs
    "web-design-india": "https://clutch.co/in/web-designers",
    "mobile-dev-india": "https://clutch.co/in/developers/mobile",
    "seo-india": "https://clutch.co/in/agencies/seo",
    "software-dev-india": "https://clutch.co/in/developers",
    "cloud-consulting-india": "https://clutch.co/in/cloud-consulting",
    
    # Digital Marketing URLs
    "digital-marketing": "https://clutch.co/agencies/digital-marketing",
    "seo": "https://clutch.co/agencies/seo",
    "ppc": "https://clutch.co/agencies/ppc",
    "social-media": "https://clutch.co/agencies/social-media-marketing",
    "content-marketing": "https://clutch.co/agencies/content-marketing",
    
    # Digital Design URLs
    "web-design": "https://clutch.co/web-designers",
    "ui-ux": "https://clutch.co/agencies/ui-ux",
    "graphic-design": "https://clutch.co/agencies/graphic-designers",
    "logo-design": "https://clutch.co/agencies/logo-designers",
    "product-design": "https://clutch.co/agencies/product-design",
    "branding": "https://clutch.co/agencies/branding",
    
    # 🌟 Existing India-specific categories
    "digital-design-india": "https://clutch.co/in/agencies/digital-design",
    
    # 🌟🌟 NEW India-specific categories
    "blockchain-india": "https://clutch.co/in/developers/blockchain",
    "game-dev-india": "https://clutch.co/in/developers/game-development",
    "ecommerce-india": "https://clutch.co/in/developers/ecommerce",
    "digital-strategy-india": "https://clutch.co/in/agencies/digital-strategy",
    "video-production-india": "https://clutch.co/in/agencies/video-production",
    "content-marketing-india": "https://clutch.co/in/agencies/content-marketing",
    "branding-india": "https://clutch.co/in/agencies/branding",
    "ppc-india": "https://clutch.co/in/agencies/ppc",
    "social-media-india": "https://clutch.co/in/agencies/social-media-marketing",
    "ui-ux-india": "https://clutch.co/in/agencies/ui-ux",
}

# Category names for display
category_names = {
    "developers": "Software Developers",
    "mobile": "Mobile Developers",
    "web": "Web Developers",
    "ai": "AI Developers",
    "cloud-consulting": "Cloud Consulting",
    "it-services": "IT Services",
    "python": "Python Developers",
    "javascript": "JavaScript Developers",
    "react": "React Developers",
    "devops": "DevOps Companies",
    "digital-marketing": "Digital Marketing Agencies",
    "seo": "SEO Agencies",
    "ppc": "PPC Agencies",
    "social-media": "Social Media Marketing",
    "content-marketing": "Content Marketing",
    "web-design": "Web Design Companies",
    "ui-ux": "UI/UX Design Agencies",
    "graphic-design": "Graphic Designers",
    "logo-design": "Logo Design Agencies",
    "product-design": "Product Design Companies",
    "branding": "Branding Agencies",
    
    # 🌟 Existing India-specific display names
    "digital-design-india": "Digital Design Agencies in India",
    "web-design-india": "Web Design Companies in India",
    "mobile-dev-india": "Mobile Developers in India",
    "seo-india": "SEO Agencies in India",
    "software-dev-india": "Software Developers in India",
    "cloud-consulting-india": "Cloud Consulting in India",
    
    # 🌟🌟 NEW India-specific display names
    "blockchain-india": "Blockchain Developers in India",
    "game-dev-india": "Game Development Companies in India",
    "ecommerce-india": "E-commerce Developers in India",
    "digital-strategy-india": "Digital Strategy Agencies in India",
    "video-production-india": "Video Production Companies in India",
    "content-marketing-india": "Content Marketing Agencies in India",
    "branding-india": "Branding Agencies in India",
    "ppc-india": "PPC Agencies in India",
    "social-media-india": "Social Media Marketing in India",
    "ui-ux-india": "UI/UX Design Agencies in India",
}

# Define which categories should use the real scraper
REAL_SCRAPER_CATEGORIES = [
    # Existing India categories
    "digital-design-india",
    "web-design-india",
    "mobile-dev-india", 
    "seo-india",
    "software-dev-india",
    "cloud-consulting-india",
    
    # 🌟🌟 NEW India categories
    "blockchain-india",
    "game-dev-india",
    "ecommerce-india",
    "digital-strategy-india",
    "video-production-india",
    "content-marketing-india",
    "branding-india",
    "ppc-india",
    "social-media-india",
    "ui-ux-india",
]

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "name": "Clutch.co Lead Finder API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": [
            "/api/leads",
            "/api/stats",
            "/api/scrape",
            "/api/categories"
        ]
    }

@app.get("/api/leads")
async def get_leads(
    limit: int = 50,
    quality: Optional[str] = None,
    category: Optional[str] = None,
    min_score: int = 0,
    min_employees: Optional[int] = None
):
    """Get leads with filters - FIXED EMPLOYEE FILTERING"""
    
    print(f"📥 API /leads called with category: '{category}', quality: '{quality}', min_score: {min_score}, min_employees: {min_employees}")
    
    try:
        # For India-specific categories, try to get real data first
        if category and category.endswith('-india'):
            latest = get_latest_file(f"*{category}*.csv")
            if latest and os.path.exists(latest):
                try:
                    df = pd.read_csv(latest)
                    print(f"✅ Found real data file: {latest} with {len(df)} rows")
                    
                    # Apply filters
                    filtered_df = df.copy()
                    
                    if quality and 'quality' in filtered_df.columns:
                        filtered_df = filtered_df[filtered_df['quality'] == quality]
                    
                    if min_score > 0 and 'validation_score' in filtered_df.columns:
                        filtered_df['validation_score'] = pd.to_numeric(filtered_df['validation_score'], errors='coerce')
                        filtered_df = filtered_df[filtered_df['validation_score'] >= min_score]
                    
                    # Apply employee filter
                    if min_employees:
                        if 'employee_count' in filtered_df.columns:
                            filtered_df = filtered_df[filtered_df['employee_count'] >= min_employees]
                            print(f"🎯 Applied employee_count filter: {min_employees}+, got {len(filtered_df)} leads")
                        elif 'employees' in filtered_df.columns:
                            filtered_df['emp_min'] = filtered_df['employees'].apply(extract_min_employee_count)
                            filtered_df = filtered_df[filtered_df['emp_min'] >= min_employees]
                            print(f"🎯 Applied employees text filter: {min_employees}+, got {len(filtered_df)} leads")
                    
                    # Convert to records and clean NaN values
                    leads = filtered_df.head(limit).to_dict('records')
                    for lead in leads:
                        for key, value in lead.items():
                            if pd.isna(value):
                                lead[key] = None
                    
                    return {
                        "total": len(filtered_df),
                        "leads": leads,
                        "source": "real_data"
                    }
                except Exception as e:
                    print(f"⚠️ Error reading real data: {e}")
                    traceback.print_exc()
                    # Fall through to mock data
        
        # For all other categories or if real data failed, generate mock data
        print(f"🔧 Generating mock data")
        
        # Get the display name for the category
        category_name = "Companies"
        if category and category in category_names:
            category_name = category_names[category]
        elif category:
            for cat_id, cat_name in category_names.items():
                if cat_id == category:
                    category_name = cat_name
                    break
        
        print(f"📋 Using category name: '{category_name}'")
        
        # Generate mock leads
        mock_leads = generate_mock_leads(category_name, 30)
        print(f"✅ Generated {len(mock_leads)} mock leads")
        
        # Apply filters to mock data
        filtered_leads = mock_leads.copy()
        
        if quality:
            filtered_leads = [l for l in filtered_leads if l.get('quality') == quality]
            print(f"🎯 After quality filter: {len(filtered_leads)}")
        
        if min_score > 0:
            filtered_leads = [l for l in filtered_leads if l.get('validation_score', 0) >= min_score]
            print(f"🎯 After score filter: {len(filtered_leads)}")
        
        # Apply employee filter to mock data
        if min_employees:
            filtered_leads = [l for l in filtered_leads if l.get('employee_count', 0) >= min_employees]
            print(f"🎯 After employee filter ({min_employees}+): {len(filtered_leads)}")
        
        # Sort by score
        filtered_leads.sort(key=lambda x: x.get('validation_score', 0), reverse=True)
        
        result = {
            "total": len(filtered_leads),
            "leads": filtered_leads[:limit],
            "source": "mock_data"
        }
        
        print(f"📤 Returning {len(result['leads'])} leads (total: {result['total']})")
        return result
        
    except Exception as e:
        print(f"❌ CRITICAL ERROR in /api/leads: {e}")
        traceback.print_exc()
        return {
            "total": 0,
            "leads": [],
            "source": "error",
            "error": str(e)
        }

@app.get("/api/stats")
async def get_stats():
    """Get statistics about the leads with varied data for testing"""
    try:
        latest = get_latest_file("*.csv")
        
        # If no real data, return enhanced mock stats with variety
        if not latest:
            return {
                "total_leads": 157,
                "quality_breakdown": {
                    "Premium": 42,
                    "High": 58,
                    "Medium": 37,
                    "Low": 20
                },
                "categories": {
                    "Software Developers": 45,
                    "Web Developers": 32,
                    "Mobile Developers": 28,
                    "AI Developers": 25,
                    "Digital Design Agencies in India": 20,
                    "SEO Agencies": 18,
                    "PPC Agencies in India": 15,
                    "Blockchain Developers in India": 12,
                    "Game Development Companies in India": 10,
                    "E-commerce Developers in India": 8,
                    "Cloud Consulting in India": 7,
                    "UI/UX Design Agencies in India": 5
                },
                "avg_score": 84.3,
                "source": "mock_data"
            }
        
        # Try to read real data
        df = pd.read_csv(latest)
        df = df.fillna("")
        
        stats = {
            "total_leads": len(df),
            "quality_breakdown": {},
            "categories": {},
            "avg_score": 0
        }
        
        # Calculate quality breakdown
        if 'quality' in df.columns:
            quality_counts = df['quality'].value_counts()
            stats['quality_breakdown'] = quality_counts.to_dict()
        
        # Calculate category distribution
        if 'category' in df.columns:
            category_counts = df['category'].value_counts().head(10)
            stats['categories'] = category_counts.to_dict()
        
        # Calculate average score
        if 'validation_score' in df.columns:
            scores = pd.to_numeric(df['validation_score'], errors='coerce')
            stats['avg_score'] = float(scores.mean()) if not scores.isna().all() else 0
        
        return stats
        
    except Exception as e:
        print(f"Error in /api/stats: {e}")
        traceback.print_exc()
        # Return varied mock stats on error
        return {
            "total_leads": 157,
            "quality_breakdown": {
                "Premium": 42,
                "High": 58,
                "Medium": 37,
                "Low": 20
            },
            "categories": {
                "Software Developers": 45,
                "Web Developers": 32,
                "Mobile Developers": 28,
                "AI Developers": 25,
                "Digital Design Agencies in India": 20,
                "SEO Agencies": 18,
                "PPC Agencies in India": 15,
                "Blockchain Developers in India": 12,
                "Game Development Companies in India": 10,
                "E-commerce Developers in India": 8
            },
            "avg_score": 84.3,
            "error": str(e)
        }

@app.get("/api/categories")
async def get_categories():
    """Get all available categories including digital marketing and design"""
    categories = []
    for cat_id, cat_name in category_names.items():
        categories.append({"id": cat_id, "name": cat_name})
    return categories

@app.post("/api/scrape")
async def start_scrape(request: ScrapeRequest, background_tasks: BackgroundTasks):
    """Start a scraping job - ENHANCED VERSION"""
    
    print(f"📥 Received scrape request: {request.category}, {request.pages} pages")
    
    # Check if category exists
    if request.category not in urls:
        print(f"❌ Invalid category: {request.category}")
        return {
            "status": "error",
            "message": f"Invalid category: {request.category}"
        }
    
    # Get category name for display
    category_display = category_names.get(request.category, request.category)
    
    # Check if this category should use the real scraper
    use_real_scraper = request.category in REAL_SCRAPER_CATEGORIES
    
    def scrape_job():
        """Background scraping job"""
        print(f"🕷️ Background scraping started for {category_display}")
        
        try:
            if use_real_scraper:
                try:
                    from scraper.clutch_scraper import ClutchScraper
                    scraper = ClutchScraper(headless=True, slow_mode=True, extract_emails=True)
                    companies = scraper.scrape_multiple_pages(
                        base_url=urls[request.category],
                        max_pages=request.pages
                    )
                    
                    if companies:
                        for company in companies:
                            company['category'] = category_display
                            company['category_id'] = request.category
                        
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        filename = f"data/real_scraped_{request.category}_{timestamp}.csv"
                        
                        df = pd.DataFrame(companies)
                        df.to_csv(filename, index=False)
                        print(f"✅ Saved {len(companies)} companies to {filename}")
                        
                        # Count emails found
                        emails_found = sum(1 for c in companies if c.get('emails'))
                        if emails_found > 0:
                            print(f"📧 Found emails for {emails_found} companies")
                    scraper.close()
                except Exception as e:
                    print(f"❌ Real scraper error: {e}")
                    traceback.print_exc()
                    # Fallback to mock
                    mock_data = generate_mock_leads(category_display, request.pages * 10)
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    filename = f"data/fallback_{request.category}_{timestamp}.csv"
                    pd.DataFrame(mock_data).to_csv(filename, index=False)
                    print(f"✅ Fallback: Saved {len(mock_data)} mock companies")
            else:
                # Mock scraping
                time.sleep(2)
                mock_data = generate_mock_leads(category_display, request.pages * 10)
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"data/mock_{request.category}_{timestamp}.csv"
                pd.DataFrame(mock_data).to_csv(filename, index=False)
                print(f"✅ Mock: Saved {len(mock_data)} companies")
                
        except Exception as e:
            print(f"❌ Scrape job error: {e}")
            traceback.print_exc()
        
        print(f"✅ Background scraping completed for {category_display}")
    
    # Add to background tasks
    background_tasks.add_task(scrape_job)
    
    # Return consistent success response
    return {
        "status": "success",
        "data": {
            "job_id": f"job_{request.category}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            "category": request.category,
            "category_name": category_display,
            "pages": request.pages,
            "message": f"Successfully started scraping {category_display} for {request.pages} pages",
            "scraper_type": "real" if use_real_scraper else "mock"
        }
    }

# Run with: uvicorn main:app --reload --port 8001
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, reload=True)