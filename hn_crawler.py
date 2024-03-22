import os
import sqlite3
import requests
import json
from datetime import datetime, timedelta
import argparse


import os
import sqlite3
import requests
import json
from datetime import datetime, timedelta


class HNCrawler:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = self._init_db()
        self._create_dirs()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''
        CREATE TABLE IF NOT EXISTS crawler_meta (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT,
            identifier TEXT,
            status TEXT,
            timestamp DATETIME
        )''')
        c.execute('''
        CREATE TABLE IF NOT EXISTS data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT,
            content TEXT
        )''')
        conn.commit()
        return conn

    def _create_dirs(self):
        os.makedirs('stories', exist_ok=True)
        os.makedirs('comments', exist_ok=True)

    def fetch_stories(self, query: str, days_ago: int = 30, pages: int = 5):
        base_url = "https://hn.algolia.com/api/v1/search"
        today = datetime.now()
        one_month_ago = int((today - timedelta(days=days_ago)).timestamp())
        for page in range(pages):
            url = f"{base_url}?query={query}&tags=story&numericFilters=created_at_i>{one_month_ago}&page={page}"
            response = requests.get(url)
            if response.status_code == 200:
                stories = response.json()['hits']
                for story in stories:
                    self._store_story(story)
                    self._schedule_comments_for_crawling(story['objectID'], story.get('children', []))
            else:
                print(f"Failed to fetch stories for page {page}")

    def _store_story(self, story: dict):
        story_id = story['objectID']
        with open(f'stories/{story_id}.json', 'w') as f:
            json.dump(story, f)
        c = self.conn.cursor()
        c.execute("INSERT INTO data (type, content) VALUES (?, ?)", ("story", json.dumps(story)))
        self.conn.commit()

    def _schedule_comments_for_crawling(self, story_id: str, comment_ids: list):
        c = self.conn.cursor()
        for comment_id in comment_ids:
            c.execute("INSERT OR IGNORE INTO crawler_meta (type, identifier, status, timestamp) VALUES (?, ?, ?, ?)", 
                      ("comment", comment_id, "scheduled", datetime.now()))
        self.conn.commit()
        self.fetch_and_store_comments(comment_ids)

    def fetch_and_store_comments(self, comment_ids: list):
        while comment_ids:
            current_id = comment_ids.pop(0)
            self._fetch_and_store_comment(current_id, comment_ids)

    def _fetch_and_store_comment(self, comment_id: str, all_comment_ids: list):
        url = f"https://hacker-news.firebaseio.com/v0/item/{comment_id}.json"
        response = requests.get(url)
        if response.status_code == 200:
            comment = response.json()
            with open(f'comments/{comment_id}.json', 'w') as f:
                json.dump(comment, f)
            c = self.conn.cursor()
            c.execute("INSERT INTO data (type, content) VALUES (?, ?)", ("comment", json.dumps(comment)))
            c.execute("UPDATE crawler_meta SET status='successful' WHERE identifier=?", (comment_id,))
            self.conn.commit()
            if 'kids' in comment:
                for kid in comment['kids']:
                    if str(kid) not in all_comment_ids:
                        all_comment_ids.append(str(kid))
                        c.execute("INSERT OR IGNORE INTO crawler_meta (type, identifier, status, timestamp) VALUES (?, ?, ?, ?)", 
                                  ("comment", kid, "scheduled", datetime.now()))
                        self.conn.commit()
        else:
            print(f"Failed to fetch comment {comment_id}")


def parse_args():
    parser = argparse.ArgumentParser(description="Hacker News Crawler")
    parser.add_argument("--query", type=str, required=True, help="Search query for stories")
    parser.add_argument("--days", type=int, default=30, help="Number of days ago to search for stories")
    parser.add_argument("--pages", type=int, default=5, help="Number of pages of search results to fetch")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()

    # Initialize the crawler with the SQLite database path
    crawler = HNCrawler(db_path="hn_data.db")

    # Fetch and store stories based on the provided CLI arguments
    print("Fetching stories...")
    crawler.fetch_stories(query=args.query, days_ago=args.days, pages=args.pages)

    # After stories have been fetched and stored, fetch and store comments
    # This part is now integrated into the fetch_stories method for each story,
    # so it's automatically handled there. If you wish to manually trigger fetching
    # of any additional comments discovered later, you could call it separately:
    # print("Fetching comments...")
    # crawler.fetch_and_store_comments()

    print("Crawling completed successfully.")