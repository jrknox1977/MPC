import feedparser

url = 'https://www.newegg.com/Product/RSS.aspx?Submit=RSSCategorydeals&Depa=0&Category=9&NAME=Computer-Cases'
entries = []

for entry in feed['entries']:
    entries.append({'title': entry['title'], 'link': entry['link']})

for item in entries:
    print(str(item['title']) + " " + str(item['link']))






