import os
import requests
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from flask import Flask, render_template, request, redirect, url_for, jsonify, session
from flask_session import Session
from datetime import datetime,timedelta
import json

from threading import Thread
from time import sleep
import logging

# Load environment variable
load_dotenv()


# Headers(Token) for GitHub API requests

def set_headers(TOKEN):
    return {
        "Authorization": f"Bearer {TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
    }

github_events = {
    "IssuesEvent",
    "PullRequestEvent",
    "PullRequestReviewEvent",
    "PushEvent"
}

# MongoDB connection --------------
MONGO_URI = os.getenv('MONGO_URI')
client = MongoClient(MONGO_URI)  
db = client['dashboard']  


# FLASK APP ---------------------
app = Flask(__name__)




# DATE FUNCTION ------------>

def get_start_date():

    today = datetime.today()
    one_year_back = today - timedelta(days=365)

    return one_year_back


def get_commit_details_from_SHA(repo_full_name, sha):

    url = f"{session['BASE_URL']}/repos/{repo_full_name}/commits/{sha}"
    response = requests.get(url, headers=session['HEADERS'])

    if response.status_code == 200:
        commit_data = response.json()

        if commit_data['commit']['message'][:12] == 'Merge branch':
            merged = True
        else:
            merged = False

        return {
            "sha": commit_data["sha"],
            "message": commit_data["commit"]["message"],
            "date": commit_data["commit"]["committer"]["date"],
            "url": commit_data["html_url"],
            "author": commit_data["commit"]["author"]["name"],
            "merged": merged,
            "stats": commit_data["stats"],
            "files": [{"filename": file['filename'], "additions": file['additions'], "deletions": file['deletions']}
                        for file in commit_data['files']]
        }
    else:
        print(f"Error fetching commit details for {sha}: {response.status_code} {response.text}")
        return None



# -- GROUP
def get_pr_details_commits_comments(repo_full_name, username, start_date):

    base_url = f"{session['BASE_URL']}/repos/{repo_full_name}"
    pull_details_list = []
    page = 1
    per_page = 100  # Adjust the number of results per page if necessary

    def get_paginated_data(url):
        """Fetch paginated data from a given URL."""
        data = []
        page = 1
        per_page = 100  # Number of results per page

        while True:
            paginated_url = f"{url}?per_page={per_page}&page={page}"
            response = requests.get(paginated_url, headers=session['HEADERS'])
            
            if response.status_code != 200:
                print(f"Error fetching data from {paginated_url}: {response.json()}")
                break

            page_data = response.json()
            if not page_data:  # Break if no more data
                break

            data.extend(page_data)
            page += 1

        return data
        
    def get_pr_commits(pr_number, username):
        detailed_commits = []

        commits_url = f"{session['BASE_URL']}/repos/{repo_full_name}/pulls/{pr_number}/commits"
        commits = get_paginated_data(commits_url)
        
        # Filter commits by username
        filtered = [commit['sha'] for commit in commits if commit['author'] and commit['author']['login'] == username]

        for sha in filtered:
            details = get_commit_details_from_SHA(repo_full_name,sha)

            if details:
                detailed_commits.append(details)
        
        return detailed_commits

    def get_pr_comments(pr_number, username):

        comments_data = []

        review_url = f"{session['BASE_URL']}/repos/{repo_full_name}/pulls/{pr_number}/reviews"
        reviews = get_paginated_data(review_url)

        for review in reviews:
            if review['user']['login'] == username:
                state = review['state']

                if state == 'APPROVED':
                    data = {
                        'state': "approved",
                        'url': review['html_url'],
                        'comment': review['body'] if review['body'] else None,
                        'date': review['submitted_at'],
                    }
                    comments_data.append(data)

                elif state in ('CHANGES_REQUESTED', 'COMMENTED'):
                    comment_url = review_url + f"/{review['id']}/comments"
                    comments = get_paginated_data(comment_url)

                    for comment in comments:
                        if comment['user']['login'] == username:

                            data = {
                                'state': state.lower(),
                                'url': comment['html_url'],
                                'comment': comment.get('body'),
                                'date': comment['updated_at'],
                                'file': comment.get('path')
                            }
                            comments_data.append(data)

        return comments_data
        
    # ------------------------------


    while True:
        # Step 1: Get all pull requests with pagination
        pulls_url = f"{base_url}/pulls?state=all&per_page={per_page}&page={page}"
        response = requests.get(pulls_url, headers=session['HEADERS'])
        
        if response.status_code != 200:
            print(f"Error fetching pull requests: {response.json()}")
            break

        pull_requests = response.json()

        # Break the loop if no more pull requests are returned
        if not pull_requests:
            break

        # Step 2: Process each pull request
        for pr in pull_requests:
            pr_author = pr['user']['login']
            assigned_by = pr['assignee']['login'] if pr.get('assignee') else None
            assigned_to = [user['login'] for user in pr.get('assignees', [])]
            pr_date = datetime.strptime(pr['created_at'], "%Y-%m-%dT%H:%M:%SZ")

            requested_reviewers = [reviewer['login'] for reviewer in pr.get('requested_reviewers', [])]

            #To HANDLE - If someone approves review, they are removed from requested_reviewers
            try:
                review_url = f"{base_url}/pulls/{pr['number']}/reviews?per_page={per_page}"
                response = requests.get(review_url, headers=session['HEADERS']).json()
                requested_reviewers += list(set(user['user']['login'] for user in response))
            except:
                pass

            # Check Date boundary
            if pr_date<start_date:
                return pull_details_list

            # Check if the author or requested reviewers match the username
            if pr_author == username or (username in requested_reviewers) or (username in assigned_to) or username==assigned_by:
                pr_number = pr['number']
                
                # Get pull request details
                pr_details = get_pr_details(repo_full_name, pr_number)
                if not pr_details:
                    continue
                
                # Get filtered commits
                print(f"Getting --> {pr_number}")
                filtered_commits = get_pr_commits(pr_number, username)
                print("Commits")
                
                # Get filtered review comments
                filtered_comments = get_pr_comments(pr_number, username)
                print("Comments")
                
                # Collect details
                pull_details_list.append({
                    "pr_number": pr_number,
                    "pr_details": pr_details,
                    "commits": filtered_commits,
                    "comments": filtered_comments,
                })

        # Increment the page number for the next request
        page += 1

    return pull_details_list

def get_pr_details(repo_full_name, pr_number):

        url = f"{session['BASE_URL']}/repos/{repo_full_name}/pulls/{pr_number}"
        response = requests.get(url, headers=session['HEADERS'])
        
        if response.status_code == 200:
            data = response.json()

            pr_details = {
                "title": data["title"],
                "number": data["number"],
                "state": data["state"],
                "merged": data["merged"],
                "url": data["html_url"],
                "date": data['created_at'],
                "requested_reviewers": [reviewer["login"] for reviewer in data["requested_reviewers"]],
                "assigned_by": data['assignee']['login'] if data.get('assignee') else None,
                "assigned_to": [user['login'] for user in data.get('assignees', [])],
                "labels": [label["name"] for label in data["labels"]],
                "comments": data["comments"],
                "review_comments": data["review_comments"],
                "commits": data["commits"],
                "additions": data["additions"],
                "deletions": data["deletions"],
                "changed_files": data["changed_files"]
        }

            return pr_details
        else:
            print(f"Error fetching PR details for #{pr_number}: {response.json()}")
            return None
# --



# UPDATE FUNCTIONS ------------------------------>

def update_repo_details(full_repo, enterprise, contributors, last_snapshot, start_date):

    # Check for Public / Enterprise --> Set Session BASE_URL and HEADERS
    if enterprise:
        BASE_URL = "https://api.github.ibm.com"
        HEADERS = set_headers(os.getenv('GITHUB_ENTERPRISE'))
    else:
        BASE_URL = "https://api.github.com"
        HEADERS = set_headers(os.getenv('GITHUB_TOKEN'))



    page = 1
    checkpoint_reached = False  # Last Saved Snapshot
    valid_date = True           # Window till start_date
    latest_snapshot_id = -1     # Initialize latest_snapshot_id to track the latest event ID

    new_updates = {}

    while (not checkpoint_reached) and valid_date and page<=3:
        event_url = f"{BASE_URL}/repos/{full_repo}/events?per_page=100&page={page}"
        response = requests.get(event_url, headers=HEADERS)
        
        if response.status_code == 200:
            data = response.json()

            # If no more events are returned, break the loop
            if not data:
                break

            # Check Valid Events
            for event in data:
                if page!=1:    # Perform below code only for the first page time 
                    break

                if (event['type'] in github_events):
                    
                    # No new data
                    if last_snapshot == event['id']:
                        print("Repo is Up to Date")
                        return
                    else:
                        # Set new snapshot
                        latest_snapshot_id = event['id']
                        break

            # Update Data -------------------
            print(f"Updating Data --> {full_repo}")

            for event in data:
                event_date = datetime.strptime(event['created_at'], "%Y-%m-%dT%H:%M:%SZ")
                username = event['actor']['login']

                # Initialize new user
                if username not in new_updates:
                    new_updates[username] = {
                        'commits': [],
                        'new_issues': [],
                        'new_prs': []
                        }

                # Invalid Event
                if (event['type'] not in github_events):
                    print("Invalid -- ", event['type'])
                    continue
                
                # Update till we reach Snapshot
                if last_snapshot == event['id']:
                    print("Checkpoint Reached <->", event['id'])
                    checkpoint_reached = True
                    break
                
                # Don't go beyond start date limit
                if event_date<start_date:
                    valid_date = False
                    break

                match event['type']:
                    case 'IssuesEvent':
                        new, (issue_no, data) = handle_issue_event(event, username)
                        print(f"issue Update -- {issue_no}")

                        if new:
                            new_updates[username]['new_issues'] += [data]

                        else:
                            # Assign the latest data
                            if issue_no and (issue_no not in new_updates[username]):
                                new_updates[username][issue_no] = data

                    case 'PullRequestEvent':
                        new, data = handle_pull_request_event(event, full_repo, username, HEADERS)

                        if new:
                            new_updates[username]['new_prs'] += [data]
                        
                        else:
                            pr_no, data = data

                            if pr_no not in new_updates[username]:
                                new_updates[username][pr_no] = {'pr_details': None, 'commits': [], 'comments': []}

                            new_updates[username][pr_no]['pr_details'] = data

                    case 'PullRequestReviewEvent':
                        pr_no,comments = handle_pull_request_review_event(event, username, HEADERS)
                        pr_details = get_pr_details(event['repo']['name'], event['payload']['pull_request']['number'])

                        if pr_no not in new_updates[username]:
                            new_updates[username][pr_no] = {'pr_details': None, 'commits': [], 'comments': []}
                        
                        new_updates[username][pr_no]['comments'] += comments

                        if pr_details:
                            new_updates[username][pr_no]['pr_details'] = pr_details

                    case 'PushEvent':
                        pr_no,commits = handle_push_event(event, full_repo, BASE_URL, HEADERS)

                        for commit in commits:
                            
                            commitor = commit['commit']['author']['name']
                            commit_data = get_commit_details_from_SHA(full_repo, commit['sha'])

                            if commitor not in new_updates:
                                new_updates[commitor] = {
                                    'commits': [],
                                    'new_issues': [],
                                    'new_prs': []
                                    }

                            if not pr_no:
                                new_updates[commitor]['commits'] += [commit_data]
                                print("Global Commit")
                                
                            else:
                                if pr_no not in new_updates[commitor]:
                                    new_updates[commitor][pr_no] = {'pr_details': None, 'commits': [], 'comments': []}

                                new_updates[commitor][pr_no]['commits'] += [commit]
                                print(f"PR Commit - {commitor}", pr_no)

                                pr_details = get_pr_details(full_repo, pr_no)
                                if pr_details:
                                    new_updates[commitor][pr_no]['pr_details'] = pr_details

                    case _:
                        print(f"Unwanted Event -- {event['type']}")

            # Increment the page number for the next request
            page += 1

        else:
            print(f"Failed to fetch events for {full_repo}, Status Code: {response.status_code}")
            return   # Exit if the request fails
    

    # If checkpoint not found -- 90 days gap  
    # if not checkpoint_reached:
    #     return 'not_found'


    # ---> Update database repo_details with the <new_updates> dict

    data_collection = db['IBM_github_data']
    repo_collection = db['IBM_repositories']

    for username in new_updates:

        # Check if valid username
        if username not in contributors:
            continue

        result = data_collection.find_one({'user_info.login':username})
        repo_details = result.get(full_repo,None) if result else None

        # This is a new repo for the given user --> perform full extraction (This should ideally not occur)
        if not repo_details:
            continue


        

        repo_details['commits'] += new_updates[username]['commits']
        repo_details['pull_requests'] += new_updates[username]['new_prs']
        repo_details['issues'] += new_updates[username]['new_issues']


        del new_updates[username]['commits']
        del new_updates[username]['new_prs']
        del new_updates[username]['new_issues']


        # Update issues
        for idx,issue in enumerate(repo_details['issues']):
            issue_no = issue['number']

            if issue_no in new_updates[username]:
                repo_details['issues'][idx] = new_updates[username][issue_no]
                del new_updates[username][issue_no]
        
        # Update PRs
        for idx,pr in enumerate(repo_details['pull_requests']):

            if str(pr['pr_number']) in new_updates[username]:
                
                pr_changes = new_updates[username][str(pr['pr_number'])]


                # If new detail changes
                if pr_changes['pr_details']:
                    repo_details['pull_requests'][idx]['pr_details'] = pr_changes['pr_details']
                
                if pr_changes['commits']:
                    repo_details['pull_requests'][idx]['commits'] = pr_changes['commits']
                
                if pr_changes['comments']:
                    repo_details['pull_requests'][idx]['comments'] += pr_changes['comments']
            
                del new_updates[username][str(pr['pr_number'])]

        # If anything remains, it is a new pull request with comments --> So append it directly
        for pr_no in new_updates[username]:
            new_updates[username][pr_no]['pr_number'] = pr_no
            repo_details['pull_requests'].append(new_updates[username][pr_no].copy())


    # DB Update -------->    
        
        # Update the github data for each user
        data_collection.update_one(
            {'user_info.login':username},
            {'$set': {full_repo : repo_details}}
            )

    # Set Latest Snapshot for Repos
    repo_collection.update_one(
        {'repo_name':full_repo},
        {'$set' : {'snapshot':latest_snapshot_id, 'last_update':datetime.today()}})

    return True

def handle_issue_event(event, username):

    issue = event['payload']['issue']        

    issue_data = {
        'url': issue['html_url'],
        'title': issue['title'],
        'number': issue['number'],
        'created_at': issue['created_at'],
        'updated_at': issue['updated_at'],
        'labels': issue['labels'],
        'state': issue['state'],
        'type': 'created' if issue['user']['login'] == username else 'assigned'
    }

    if event['payload']['action'] == 'opened':
        return True, (issue['number'], issue_data)
    else:
        return False, (issue['number'], issue_data)
    
def handle_pull_request_event(event, full_repo, username, HEADERS):

    data = event['payload']['pull_request']
    pr_details = {
            "title": data["title"],
            "number": data["number"],
            "state": data["state"],
            "merged": data["merged"],
            "url": data["html_url"],
            "date": data['created_at'],
            "requested_reviewers": [reviewer["login"] for reviewer in data["requested_reviewers"]],
            "assigned_by": data['assignee']['login'] if data.get('assignee') else None,
            "assigned_to": [user['login'] for user in data.get('assignees', [])],
            "labels": [label["name"] for label in data["labels"]],
            "comments": data["comments"],
            "review_comments": data["review_comments"],
            "commits": data["commits"],
            "additions": data["additions"],
            "deletions": data["deletions"],
            "changed_files": data["changed_files"]
        }

    if event['payload']['action'] == 'opened':
        # New pr object
        new_pr = {'pr_number': data['number'],
                  'pr_details': None,
                  'commits': [],
                  'comments': []
                }

        new_pr['pr_details'] = pr_details

        # Fetch Commits
        commits_url = data['commits_url']
        response = requests.get(commits_url, headers=HEADERS)

        if response.status_code == 200:
            fetched_commits = response.json()
        else:
            fetched_commits = []
        
        
        # Filter commits by username
        filtered = [commit['sha'] for commit in fetched_commits if commit['author'] and commit['author']['login'] == username]
        commit_details = []

        for sha in filtered:
            details = get_commit_details_from_SHA(full_repo,sha)

            if details:
                commit_details.append(details)

        new_pr['commits'] = commit_details

        return True, new_pr
        
    else:
        # Return the current PR directly
        return False, (pr_details['number'], pr_details)

def handle_pull_request_review_event(event,username,HEADERS):
    
    comments_data = []
    review = event['payload']['review']
    
    # Review Approved  |OR|  # If event has body -- then it was a single comment and no comments exist further
    if review['state'] == 'approved' or review['body']:
        data = {
            'state': review['state'],
            'url': review['html_url'],
            'comment': review['body'] if review['body'] else None,
            'date': review['submitted_at'],
        }
        comments_data.append(data)

    elif review['state'] in ('changes_requested', 'commented'):

        # Fetch all the comments
        comment_url = review['pull_request_url'] + f"/reviews/{review['id']}/comments"
        response = requests.get(comment_url, headers=HEADERS)

        if response.status_code == 200:
            fetched_comments = response.json()
        else:
            fetched_comments = []


        for comment in fetched_comments:
            if comment['user']['login'] == username:
                data = {
                        'state': review['state'],
                        'url': comment['html_url'],
                        'comment': comment.get('body'),
                        'date': comment['updated_at'],
                        'file': comment.get('path')
                    }
                comments_data.append(data)
    
    return event['payload']['pull_request']['number'],comments_data

def handle_push_event(event, full_repo, BASE_URL, HEADERS):

    commit =  event['payload']['commits'][-1]
    commit_sha = commit['sha']

    pull_url = f"{BASE_URL}/repos/{full_repo}/commits/{commit_sha}/pulls"
    response = requests.get(pull_url, headers=HEADERS)
    
    if response.status_code == 200:
        pulls = response.json()
        # If there are associated pull requests, This is valid pr_commit

        if pulls:
            pr_no = pulls[0]['number']
            commit_url = f"{BASE_URL}/repos/{full_repo}/pulls/{pr_no}/commits"
            response = requests.get(commit_url, headers=HEADERS)

            if response.status_code == 200:
                return pr_no,response.json()
            else:
                print(f"Error fetching All commits: {response.status_code} - {response.text}")


        else:
            print(f"Error fetching pull requests: {response.status_code} - {response.text}")
    
    return None,[]




def cron_job():

    user_collection = db["IBM_user_data"]
    data_collection = db["IBM_github_data"]
    mappings_collection = db['IBM_user_mappings']
    repo_collection = db['IBM_repositories']


    while True:
        print(f'Update Started <-> {datetime.today()}',flush=True)
        
        for repo in repo_collection.find({}):
            print(f"Updating -> {repo['repo_name']}",flush=True)

            repo_name = repo['repo_name']
            enterprise = repo['enterprise']
            contributors = repo['contributors']
            last_snapshot = repo['snapshot']

            start_date = get_start_date()

            # if repo_name in ('IBM/ibm-spectrum-scale-csi'):
            update_repo_details(repo_name, enterprise, contributors, last_snapshot, start_date)
        
        print('Update DONE',flush=True)
        sleep(3600)


@app.route('/')
def home():
    return jsonify({'message':'cron_job'}), 200


cron_job()
app.run(port=5000)
