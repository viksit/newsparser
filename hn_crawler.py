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
            identifier TEXT UNIQUE,
            status TEXT,
            timestamp DATETIME
        )''')
        c.execute('''
        CREATE TABLE IF NOT EXISTS data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT,
            content TEXT,
            identifier TEXT UNIQUE
        )''')
        conn.commit()
        return conn

    def _create_dirs(self):
        os.makedirs('stories', exist_ok=True)
        os.makedirs('comments', exist_ok=True)

    def _print_stats(self):
        c = self.conn.cursor()
        c.execute("SELECT COUNT(*) FROM data WHERE type='story'")
        stories_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM data WHERE type='comment'")
        comments_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM crawler_meta WHERE status='scheduled'")
        scheduled = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM crawler_meta WHERE status='successful'")
        completed = c.fetchone()[0]
        print(f"Stats - Stories: {stories_count}, Comments: {comments_count}, Scheduled: {scheduled}, Completed: {completed}")

    def fetch_stories(self, query: str, days_ago: int = 30, pages: int = 5):
        base_url = "https://hn.algolia.com/api/v1/search"
        today = datetime.now()
        one_month_ago = int((today - timedelta(days=days_ago)).timestamp())
        for page in range(pages):
            url = f"{base_url}?query={query}&tags=story&numericFilters=created_at_i>{one_month_ago}&page={page}"
            print(f"Fetching stories from page {page}")
            response = requests.get(url)
            if response.status_code == 200:
                stories = response.json()['hits']
                for story in stories:
                    self._store_story(story)
                self._print_stats()
            else:
                print(f"Failed to fetch stories for page {page}")

    def _store_story(self, story: dict):
        story_id = story['objectID']
        c = self.conn.cursor()
        c.execute("SELECT id FROM data WHERE identifier=?", (story_id,))
        if c.fetchone() is None:
            with open(f'stories/{story_id}.json', 'w') as f:
                json.dump(story, f)
            c.execute("INSERT INTO data (type, content, identifier) VALUES (?, ?, ?)", ("story", json.dumps(story), story_id))
            self.conn.commit()
            print(f"Stored story {story_id}")
        else:
            print(f"Skipping already crawled story {story_id}")

    def _schedule_comments_for_stored_stories(self):
        print("Scheduling comments for all stored stories...")
        c = self.conn.cursor()
        c.execute("SELECT identifier, content FROM data WHERE type='story'")
        for row in c.fetchall():
            story_id, content = row
            story = json.loads(content)
            if 'children' in story:
                for comment_id in story['children']:
                    if not self._comment_already_crawled(comment_id):
                        c.execute("INSERT OR IGNORE INTO crawler_meta (type, identifier, status, timestamp) VALUES (?, ?, 'scheduled', ?)", 
                                  ("comment", comment_id, datetime.now()))
                        print(f"Scheduled comment {comment_id} for crawling.")
        self.conn.commit()

    def _comment_already_crawled(self, comment_id):
        c = self.conn.cursor()
        c.execute("SELECT id FROM data WHERE identifier=?", (comment_id,))
        return c.fetchone() is not None

    # def fetch_and_store_comments(self):
    #     print("Fetching scheduled comments...")
    #     c = self.conn.cursor()
    #     c.execute("SELECT identifier FROM crawler_meta WHERE type='comment' AND status='scheduled'")
    #     comment_ids = [comment_id[0] for comment_id in c.fetchall()]

    #     while comment_ids:
    #         comment_id = comment_ids.pop(0)
    #         if not self._comment_already_crawled(comment_id):
    #             self._fetch_and_store_comment(comment_id)

    # def _fetch_and_store_comment(self, comment_id: str):
    #     url = f"https://hacker-news.firebaseio.com/v0/item/{comment_id}.json"
    #     response = requests.get(url)
    #     if response.status_code == 200:
    #         comment = response.json()
    #         with open(f'comments/{comment_id}.json', 'w') as f:
    #             json.dump(comment, f)
    #         c = self.conn.cursor()
    #         c.execute("INSERT INTO data (type, content, identifier) VALUES (?, ?, ?)", ("comment", json.dumps(comment), comment_id))
    #         c.execute("UPDATE crawler_meta SET status='successful', timestamp=? WHERE identifier=?", (datetime.now(), comment_id))
    #         self.conn.commit()
    #         print(f"Stored comment {comment_id}")

    #         # Check for nested comments and schedule them if they have not been crawled
    #         if 'kids' in comment:
    #             for kid_id in comment['kids']:
    #                 if not self._comment_already_crawled(kid_id):
    #                     c.execute("INSERT OR IGNORE INTO crawler_meta (type, identifier, status, timestamp) VALUES (?, ?, 'scheduled', ?)", 
    #                               ("comment", kid_id, datetime.now()))
    #                     comment_ids.append(kid_id)  # Add new comment ID to the list for processing
    #                     print(f"Scheduled nested comment {kid_id} for crawling.")
    #             self.conn.commit()
    #     else:
    #         print(f"Failed to fetch comment {comment_id}")
    def fetch_and_store_comments(self):
        print("Fetching scheduled comments...")
        c = self.conn.cursor()
        c.execute("SELECT identifier FROM crawler_meta WHERE type='comment' AND status='scheduled'")
        comment_ids = [comment_id[0] for comment_id in c.fetchall()]

        index = 0
        while index < len(comment_ids):
            comment_id = comment_ids[index]
            if not self._comment_already_crawled(comment_id):
                self._fetch_and_store_comment(comment_id, comment_ids)
            index += 1

    def _fetch_and_store_comment(self, comment_id: str, comment_ids: list):
        url = f"https://hacker-news.firebaseio.com/v0/item/{comment_id}.json"
        response = requests.get(url)
        if response.status_code == 200:
            comment = response.json()
            with open(f'comments/{comment_id}.json', 'w') as f:
                json.dump(comment, f)
            c = self.conn.cursor()
            c.execute("INSERT INTO data (type, content, identifier) VALUES (?, ?, ?)", ("comment", json.dumps(comment), comment_id))
            c.execute("UPDATE crawler_meta SET status='successful', timestamp=? WHERE identifier=?", (datetime.now(), comment_id))
            self.conn.commit()
            print(f"Stored comment {comment_id}")

            # Check for nested comments and schedule them if they have not been crawled
            if 'kids' in comment:
                for kid_id in comment['kids']:
                    if not self._comment_already_crawled(kid_id):
                        c.execute("INSERT OR IGNORE INTO crawler_meta (type, identifier, status, timestamp) VALUES (?, ?, 'scheduled', ?)", 
                                  ("comment", kid_id, datetime.now()))
                        comment_ids.append(kid_id)  # Add new comment ID to the list for processing
                        print(f"Scheduled nested comment {kid_id} for crawling.")
                self.conn.commit()
        else:
            print(f"Failed to fetch comment {comment_id}")

    def start_crawling(self, query: str, days_ago: int = 30, pages: int = 5):
        print("Starting crawling process...")
        self.fetch_stories(query, days_ago, pages)
        self._schedule_comments_for_stored_stories()
        self.fetch_and_store_comments()
        print("Crawling process completed.")
        self._print_stats()


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
    # crawler.fetch_stories(query=args.query, days_ago=args.days, pages=args.pages)
    crawler.start_crawling(query=args.query, days_ago=args.days, pages=args.pages)
    # After stories have been fetched and stored, fetch and store comments
    # This part is now integrated into the fetch_stories method for each story,
    # so it's automatically handled there. If you wish to manually trigger fetching
    # of any additional comments discovered later, you could call it separately:
    # print("Fetching comments...")
    # crawler.fetch_and_store_comments()

    print("Crawling completed successfully.")