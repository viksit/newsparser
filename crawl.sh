while IFS= read -r line; do
    python hn_crawler.py --query "$line" --days 14 --pages 5
done < queries.txt
