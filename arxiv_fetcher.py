import requests
import xml.etree.ElementTree as ET
from datetime import date, timedelta, datetime, timezone
import time
import json
import os
import re
import latex2mathml.converter

def get_text_list(metadata_block, element_name, namespaces):
    """Helper to extract text from all matching elements."""
    return [el.text for el in metadata_block.findall(f'dc:{element_name}', namespaces) if el.text]

def convert_latex_to_mathml(text):
    r"""
    Finds LaTeX math patterns in text ($...$ or \(...\)) and converts them to MathML.
    Returns the text with MathML replacements.
    """
    if not text:
        return text

    def replacer(match):
        latex_content = match.group(1)
        try:
            # latex2mathml produces <math ...>...</math>
            return latex2mathml.converter.convert(latex_content)
        except Exception:
            # If conversion fails, return original matched string
            return match.group(0)

    # Pattern 1: $ ... $
    # Matches $...$ but not \$... or ...\$
    p1 = r'(?<!\\)\$(.*?)(?<!\\)\$'
    text = re.sub(p1, replacer, text, flags=re.DOTALL)

    # Pattern 2: \( ... \)
    # Matches \(...\)
    p2 = r'\\\((.*?)\\\)'
    text = re.sub(p2, replacer, text, flags=re.DOTALL)

    return text

def fetch_arxiv_records(start_date: date, end_date: date, categories: list = None):
    """
    Retrieve metadata for arXiv articles published within a date range,
    optionally filtering by a list of categories (e.g., ['cs.AI', 'cs.LG']).
    """
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    cat_display = ", ".join(categories) if categories else "All"

    base_url = "https://oaipmh.arxiv.org/oai"
    params = {
        "verb": "ListRecords",
        "metadataPrefix": "oai_dc",
        "from": start_str,
        "until": end_str,
    }

    # We assume all requested categories belong to the same top-level set (e.g., 'cs').
    if categories:
        # Take the first category to determine the set, e.g. "cs.AI" -> "cs"
        top_level_set = categories[0].split('.')[0]
        params["set"] = top_level_set

    namespaces = {
        'oai': 'http://www.openarchives.org/OAI/2.0/',
        'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
        'dc': 'http://purl.org/dc/elements/1.1/'
    }

    all_records = []
    session = requests.Session()

    total_processed = 0
    max_retries = 3

    while True:
        retry_count = 0
        while retry_count < max_retries:
            try:
                response = session.get(base_url, params=params, timeout=10)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 2 ** retry_count
                    print(f"Request failed ({e}). Retrying in {wait_time}s... (attempt {retry_count}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    print(f"An error occurred during the API request after {max_retries} retries: {e}")
                    return []

        root = ET.fromstring(response.content)

        # Check for OAI-PMH errors (e.g., no records found)
        error = root.find('.//oai:error', namespaces)
        if error is not None:
            if error.get('code') == 'noRecordsMatch':
                print("No records found for this period.")
                return []
            print(f"OAI Error: {error.text}")
            return []

        records = root.findall('.//oai:record', namespaces)

        for record in records:
            header = record.find('oai:header', namespaces)
            if header.get('status') == 'deleted':
                continue

            total_processed += 1

            # Extract setSpecs from header
            set_specs = [s.text for s in header.findall('oai:setSpec', namespaces)]

            def clean_category(set_spec):
                parts = set_spec.split(':')
                if len(parts) == 3:
                    return f"{parts[1]}.{parts[2]}"
                elif len(parts) == 2:
                    return parts[1]
                else:
                    return set_spec

            categories_cleaned = sorted(list(set(clean_category(s) for s in set_specs)))

            metadata_block = record.find('.//oai_dc:dc', namespaces)
            if metadata_block is None:
                continue

            subjects = get_text_list(metadata_block, 'subject', namespaces)

            # Post-fetch filtering: match any of the requested categories
            if categories:
                matched_any = False
                for cat in categories:
                    # Check against setSpecs
                    parts = cat.split('.')
                    if len(parts) == 2:
                        archive, subject_class = parts
                        expected_suffix = f":{archive}:{subject_class}"
                        if any(s == f"{archive}:{subject_class}" or s.endswith(expected_suffix) for s in set_specs):
                            matched_any = True
                    else:
                        if any(cat in s for s in set_specs):
                            matched_any = True

                    if matched_any:
                        break

                if not matched_any:
                    continue

            datestamp = header.find('oai:datestamp', namespaces).text
            
            # Helper to get first element or default
            def get_first(key, default='N/A'):
                vals = get_text_list(metadata_block, key, namespaces)
                return vals[0] if vals else default

            raw_title = get_first('title')
            raw_desc = get_first('description')

            record_data = {
                'title': convert_latex_to_mathml(raw_title),
                'creators': get_text_list(metadata_block, 'creator', namespaces),
                'subjects': subjects,
                'categories': categories_cleaned,
                'description': convert_latex_to_mathml(raw_desc),
                'date': get_first('date'),
                'announcement_date': datestamp,
                'identifier': get_first('identifier'),
            }

            all_records.append(record_data)

        token_element = root.find('.//oai:resumptionToken', namespaces)
        if token_element is not None and token_element.text:
            params = {"verb": "ListRecords", "resumptionToken": token_element.text}
            print(f"- Found resumption token. Fetching next page... (collected {len(all_records)} so far, processed {total_processed})")
            time.sleep(3) 
        else:
            break
            
    print(f"Total records in set: {total_processed}")
    print(f"Matched {categories}: {len(all_records)}")

    return all_records

if __name__ == "__main__":
    target_categories = ["cs.AI", "cs.LG"]
    output_file = "arxiv_recent_csAILG.json"
    today = datetime.now(timezone.utc).date()
    RETENTION_DAYS = 14

    existing_articles = []
    start_date = today - timedelta(days=RETENTION_DAYS)

    prune_date = today - timedelta(days=RETENTION_DAYS)
    prune_date_str = prune_date.strftime('%Y-%m-%d')

    if os.path.exists(output_file):
        print(f"Found existing data in '{output_file}'.")
        try:
            with open(output_file, "r", encoding="utf-8") as f:
                existing_articles = json.load(f)

            if existing_articles:
                latest_entry = max(existing_articles, key=lambda x: x.get('announcement_date', ''))

                latest_date_str = latest_entry.get('announcement_date')
                if latest_date_str:
                    try:
                        last_date = date.fromisoformat(latest_date_str)
                        start_date = last_date
                        print(f"Latest article date found: {start_date}")
                    except ValueError:
                        pass
        except Exception as e:
            print(f"Error reading existing file: {e}. Starting fresh.")
            existing_articles = []

    print(f"Configuration:")
    print(f"  Categories: {target_categories}")
    print(f"  Period:     {start_date} to {today}")
    print(f"  Retention:  {RETENTION_DAYS} days (older than {prune_date_str} will be removed)")

    if start_date > today:
        start_date = today

    new_articles = fetch_arxiv_records(start_date, today, categories=target_categories)

    # merge, deduplicate, and prune
    if new_articles or existing_articles:
        if new_articles:
            print(f"Fetched {len(new_articles)} new records.")
        articles_map = {a['identifier']: a for a in existing_articles}
        for a in new_articles:
            articles_map[a['identifier']] = a
        all_articles = list(articles_map.values())

        filtered_articles = []
        for a in all_articles:
            a_date_str = a.get('date')
            if a_date_str and a_date_str >= prune_date_str:
                filtered_articles.append(a)

        # sort by publication date descending
        filtered_articles.sort(key=lambda x: x.get('date', ''), reverse=True)

        removed_count = len(all_articles) - len(filtered_articles)
        if removed_count > 0:
            print(f"Pruned {removed_count} articles older than {prune_date_str}.")

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(filtered_articles, f, indent=2, ensure_ascii=False)
        print(f"Saved {len(filtered_articles)} total results to '{output_file}'.")
    else:
        print("No articles found (new or existing).")
