# SocialMediaScraper

## Introduction
This repo contains Python scripts for scraping social media data from five platforms namely YouTube, Instagram, TikTok, LinkedIn, and X.com. Each platform's data scraping is handled by dedicated scripts designed to retrieve user profiles and posts, outputting results in both JSON and CSV formats.

## Repository Structure
The codebase is organized into separate folders for each social media platform:

- youTube/
- instagram/
- tikTok/
- linkedin/
- x/

## How to Use
Below are instructions on how to run the scripts for each platform. Ensure that you have Python installed and the necessary libraries required by the scripts. Replace placeholders (e.g., <name_of_the_handle>) with actual values. Also, please make sure to replace the variable **api_key** with your key when in production.

### YouTube
Navigate to the youTube/ directory to run these scripts.

#### Profile & Posts Scraping 
```
python youtubeAPI_profile.py <name_of_the_handle> <no_of_posts>
```

#### Example
```
python youtubeAPI_profile.py MrBeast 10
```


### Instagram
Navigate to the instagram/ directory to run these scripts.

#### Profile  & Posts Scraping
```
python instagramAPI_profile.py <instagram_handle> <no_of_posts>
```

#### Example
```
python instagramAPI_profile.py retirewithryne 10
```


### TikTok
Navigate to the tikTok/ directory to run these scripts.

#### Profile Scraping
```
python tiktokAPI_profile.py <tiktok_handle>
```

#### Example
```
python tiktokAPI_profile.py babyariel
```

#### Posts Scraping
```
python tiktokAPI_posts.py <tiktok_handle> <number_of_posts> <from_day> <from_month> <from_year>
```

#### Example
```
python tiktokAPI_posts.py babyariel 10 07 01 2024
```

### X.com
Navigate to the x/ directory to run these scripts.

#### Profile Scraping
```
python xcomAPI_profile.py <xcom_handle>
```

#### Example
```
python xcomAPI_profile.py babyariel
```

#### Posts Scraping
```
python xcomAPI_posts.py <post_id>
```

#### Example
```
python xcomAPI_posts.py 1772288221226872999
```

### LinkedIn
Navigate to the linkedIn/ directory to run this script.

#### Profile Scraping
```
python linkedinAPI_profile.py <linkedin_handle>
```

#### Example
```
python linkedinAPI_profile.py aviv-tal-75b81
```
