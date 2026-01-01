import requests
import xml.etree.ElementTree as ET
from datetime import date, timedelta
import time
import json

def get_text_list(metadata_block, element_name, namespaces):
    """Helper to extract text from all matching elements."""
    return [el.text for el in metadata_block.findall(f'dc:{element_name}', namespaces) if el.text]

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

            record_data = {
                'title': get_first('title'),
                'creators': get_text_list(metadata_block, 'creator', namespaces),
                'subjects': subjects,
                'description': get_first('description'),
                'date': get_first('date'),
                'identifier': get_first('identifier'),
            }

            # Filter out papers that are only updated (not newly published) in
            # the request window. But allow a buffer of 5 days to account for
            # submission-to-announcement lag.
            cutoff_date = (start_date - timedelta(days=5)).strftime('%Y-%m-%d')
            if record_data['date'] < cutoff_date:
                continue

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
    today = date.today()
    week_ago = today - timedelta(days=7)
    
    print(f"Configuration:")
    print(f"  Categories: {target_categories}")
    print(f"  Period:     {week_ago} to {today}")
    
    articles = fetch_arxiv_records(week_ago, today, categories=target_categories)
    
    if articles:
        output_file = "arxiv_recent_csAILG.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(articles, f, indent=2, ensure_ascii=False)
        print(f"Saved results to '{output_file}'.")
    else:
        print(f"No articles found.")
