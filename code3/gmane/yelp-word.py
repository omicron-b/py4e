import sqlite3
import time
import string

conn = sqlite3.connect('yelp.sqlite')
conn2 = sqlite3.connect('yelp-temp.sqlite')
cur = conn.cursor()
cur2 = conn2.cursor()

cur2.execute('DROP TABLE IF EXISTS Analysis')
cur2.execute('''CREATE TABLE IF NOT EXISTS Analysis
    (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, business_id TEXT,
    category_name TEXT, words TEXT)''')

# Select desired category here
cur.execute('''SELECT Reviews.business_id, Reviews.review, Categories.name
    FROM Reviews JOIN Bis_Cats JOIN Categories
    ON Bis_Cats.business_id = Reviews.business_id AND
    Bis_Cats.category_id = Categories.id
    WHERE Categories.id = 62
    LIMIT 200000''')

# Blacklist small common words
blacklisted = {'they', 'were', 'this', 'have', 'with', 'that', 'their', 'there', 'them', 'when', 'what', 'here', 'went',\
'will', 'would', 'than', 'then', 'didnt', 'dont', 'which'}

count = 0

# Clean and copy data for analysis to a temp database
for row in cur:
    count += 1
    print(count) # Track progress
    words_per_b = []
    review = row[1]
    review = review.translate(str.maketrans('','',string.punctuation))
    review = review.translate(str.maketrans('','','1234567890'))
    review = review.strip().lower()
    words = review.split()
    for word in words:
        if len(word) < 4 : continue
        if word in blacklisted : continue
        else:
            words_per_b.append(word)
    words_text = ','.join(words_per_b)
    cur2.execute('''INSERT OR IGNORE INTO Analysis (business_id, category_name, words) VALUES (?, ?, ?)
            ''', ( row[0], row[2], words_text ))
conn2.commit()

# Create dictionary from temp database
# Code below repeats gword.py
cur2.execute('SELECT words FROM Analysis')
words_counts = {}
for row in cur2 :
    words = row[0].split(',')
    for word in words:
        words_counts[word] = words_counts.get(word,0) + 1

cur2.close()
cur.close()

x = sorted(words_counts, key=words_counts.get, reverse=True)
highest = None
lowest = None
for k in x[:100]:
    if highest is None or highest < words_counts[k] :
        highest = words_counts[k]
    if lowest is None or lowest > words_counts[k] :
        lowest = words_counts[k]
print('Range of counts:',highest,lowest)

# Spread the font sizes across 20-100 based on the count
bigsize = 80
smallsize = 20

fhand = open('yelpword.js','w')
fhand.write("yelpword = [")
first = True
for k in x[:100]:
    if not first : fhand.write( ",\n")
    first = False
    size = words_counts[k]
    size = (size - lowest) / float(highest - lowest)
    size = int((size * bigsize) + smallsize)
    fhand.write("{text: '"+k+"', size: "+str(size)+"}")
fhand.write( "\n];\n")
fhand.close()

print("Output written to yelpword.js")
print("Open yelpword.htm in a browser to see the vizualization")
