
# This software is licenced under the BSD 3-Clause licence
# available at https://opensource.org/licenses/BSD-3-Clause
# and described in the LICENCE file in the root of this project

"""
Example Python 3 application using the dspace.py API client library to retrieve basic person
and group information in a DSpace repository
"""

import logging
import os
import sys

from dspace_rest_client.client import DSpaceClient

# Import models as below if needed
# from dspace_rest_d.models import Community, Collection, Item, Bundle, Bitstream

# Example variables needed for authentication and basic API requests
# SET THESE TO MATCH YOUR TEST SYSTEM BEFORE RUNNING THE EXAMPLE SCRIPT
# You can also leave them out of the constructor and set environment variables instead:
# DSPACE_API_ENDPOINT=
# DSPACE_API_USERNAME=
# DSPACE_API_PASSWORD=
# USER_AGENT=
DEFAULT_URL = "https://localhost:8080/server/api"
DEFAULT_USERNAME = "username@test.system.edu"
DEFAULT_PASSWORD = "password"

GROUP_UUID = "UUID_OF_GROUP_TO_FETCH"
NEW_GROUP_NAME = "New Test Group"
UPDATED_GROUP_NAME = "Updated Test Group"
PARENT_GROUP_UUID = "UUID_OF_PARENT_GROUP"
CHILD_GROUP_UUID = "UUID_OF_CHILD_GROUP"
EPERSON_UUID = "UUID_OF_EPERSON_TO_FETCH"
QUERY = "Administrator"
SEARCH_EMAIL = "username@test.system.edu"
SEARCH_PERSON_QUERY = "Test"
SEARCH_GROUP_QUERY = "Administrator"

# Configuration from environment variables
URL = os.environ.get("DSPACE_API_ENDPOINT", DEFAULT_URL)
USERNAME = os.environ.get("DSPACE_API_USERNAME", DEFAULT_USERNAME)
PASSWORD = os.environ.get("DSPACE_API_PASSWORD", DEFAULT_PASSWORD)

# Instantiate DSpace client
# Note the 'fake_user_agent' setting here -- this will set a string like the following,
# to get by Cloudfront:
# Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) \
# Chrome/39.0.2171.95 Safari/537.36
# The default is to *not* fake the user agent, and instead use the default of
# DSpace-Python-REST-Client/x.y.z.
# To specify a custom user agent, set the USER_AGENT env variable and leave/set
# fake_user_agent as False
d = DSpaceClient(
    api_endpoint=URL, username=USERNAME, password=PASSWORD, fake_user_agent=True
)

# Authenticate against the DSpace client
authenticated = d.authenticate()
if not authenticated:
    print("Error logging in! Giving up.")
    sys.exit(1)

# --- USER FUNCTIONS ---

# Get users with pagination
logging.info("Fetching users...")
users = d.get_users(page=0, size=5)
for user in users:
    print(f"User: {user.uuid}, Name: {user.name}, Email: {user.email}")

# Get users using an iterator
logging.info("Fetching users via iterator...")
user_iter = d.get_users_iter()
for user in user_iter:
    print(f"Iterated User: {user.uuid}, Name: {user.name}, Email: {user.email}")

# Get a user by UUID
logging.info("Fetching user by UUID: %s", EPERSON_UUID)
user = d.get_user_by_uuid(EPERSON_UUID)
if user:
    print(f"Fetched User: {user.uuid}, Name: {user.name}, Email: {user.email}")

# Search for a user by email
logging.info("Searching user by email: %s", SEARCH_EMAIL)
user = d.search_user_by_email(SEARCH_EMAIL)
if user:
    print(f"Found User: {user.uuid}, Name: {user.name}, Email: {user.email}")

# Search users by metadata
logging.info("Searching users by metadata: %s", SEARCH_PERSON_QUERY)
users = d.search_users_by_metadata(query=SEARCH_PERSON_QUERY)
for user in users:
    print(f"Matched User: {user.uuid}, Name: {user.name}, Email: {user.email}")

# --- GROUP FUNCTIONS ---

# Get groups with pagination
logging.info("Fetching groups...")
groups = d.get_groups(page=0, size=5)
for group in groups:
    print(f"Group: {group.uuid}, Name: {group.name}")

# Get a group by UUID
logging.info("Fetching group by UUID: %s", GROUP_UUID)
group = d.get_group_by_uuid(GROUP_UUID)
if group:
    print(f"Fetched Group: {group.uuid}, Name: {group.name}")

# Create a new group
logging.info("Creating a new group...")
new_group = d.create_group({"name": NEW_GROUP_NAME})
print(new_group)
if new_group is not None:
    print(f"Created Group: {new_group.uuid}, Name: {new_group.name}")
    # Update group name
    new_group_uuid = new_group.uuid
    logging.info("Updating group name for %s...", new_group_uuid)
    updated_group = d.update_group_name(new_group_uuid, UPDATED_GROUP_NAME)
    if updated_group:
        print(f"Updated Group: {updated_group.uuid}, Name: {updated_group.name}")
else:
    print("""Error creating group! This may be due to a group with the same name already existing.
          There is no update of the group name in this case.""")

# Add a subgroup
logging.info("Adding subgroup %s to %s...", CHILD_GROUP_UUID, PARENT_GROUP_UUID)
if d.add_subgroup(PARENT_GROUP_UUID, CHILD_GROUP_UUID):
    print(f"Subgroup {CHILD_GROUP_UUID} added to {PARENT_GROUP_UUID}")

# Fetch subgroups
logging.info("Fetching subgroups of %s...", PARENT_GROUP_UUID)
subgroups = d.get_subgroups(PARENT_GROUP_UUID)
for subgroup in subgroups:
    print(f"Subgroup: {subgroup.uuid}, Name: {subgroup.name}")

# Remove a subgroup
logging.info("Removing subgroup %s from %s...", CHILD_GROUP_UUID, PARENT_GROUP_UUID)
if d.remove_subgroup(PARENT_GROUP_UUID, CHILD_GROUP_UUID):
    print(f"Subgroup {CHILD_GROUP_UUID} removed from {PARENT_GROUP_UUID}")

# Search groups by metadata
logging.info("Searching groups by metadata: %s", QUERY)
found_groups = d.search_groups_by_metadata(QUERY)
for group in found_groups:
    print(f"Matched Group: {group.uuid}, Name: {group.name}")

# Get EPersons in a group
logging.info("Fetching EPersons in group %s...", GROUP_UUID)
epersons = d.get_epersons_in_group(GROUP_UUID)
for eperson in epersons:
    print(f"EPerson: {eperson.uuid}, Name: {eperson.name}, Email: {eperson.email}")

# Add an EPerson to a group
logging.info("Adding EPerson %s to group %s...", EPERSON_UUID, GROUP_UUID)
if d.add_eperson_to_group(GROUP_UUID, EPERSON_UUID):
    print(f"EPerson {EPERSON_UUID} added to group {GROUP_UUID}")

# Remove an EPerson from a group
logging.info("Removing EPerson %s from group %s...", EPERSON_UUID, GROUP_UUID)
if d.remove_eperson_from_group(GROUP_UUID, EPERSON_UUID):
    print(f"EPerson {EPERSON_UUID} removed from group {GROUP_UUID}")

# Create a new person record
user = {
    "canLogIn": True,
    "email": "user@institution.edu",
    "requireCertificate": False,
    "metadata": {
        "eperson.firstname": [{"value": "Test"}],
        "eperson.lastname": [{"value": "Dummy"}],
    },
}
logging.info("Creating a new person record...")
new_person = d.create_user(user)
if new_person:
    print(f"Created Person: {new_person.uuid}, Name: {new_person.name}")
