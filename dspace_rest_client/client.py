# This software is licenced under the BSD 3-Clause licence
# available at https://opensource.org/licenses/BSD-3-Clause
# and described in the LICENSE.txt file in the root of this project

"""
DSpace REST API client library. Intended to make interacting with DSpace in Python 3 easier, 
particularly when creating, updating, retrieving and deleting DSpace Objects.
This client library is a work in progress and currently only implements the most basic 
functionality.
It was originally created to assist with a migration of container structure, items and bistreams 
from a non-DSpace system to a new DSpace 7 repository.

It needs a lot of expansion: resource policies and permissions, validation of prepared objects 
and responses, better abstracting and handling of HAL-like API responses, plus just all the other 
endpoints and operations implemented.

@author Kim Shepherd <kim@shepherd.nz>
"""
import json
import logging
import functools
import os
from uuid import UUID
from urllib.parse import urlparse

import requests
from requests import Request
import pysolr

from .models import (
    SimpleDSpaceObject,
    Community,
    Collection,
    Item,
    Bundle,
    Bitstream,
    User,
    Group,
    DSpaceObject,
)
from . import __version__

__all__ = ["DSpaceClient"]

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)


def parse_json(response):
    """
    Simple static method to handle ValueError if JSON is invalid in response body
    @param response: the http response object (which should contain JSON)
    @return: parsed JSON object
    """
    response_json = None
    try:
        if response is not None:
            response_json = response.json()
    except ValueError as err:
        if response is not None:
            logging.error(
                "Error parsing response JSON: %s. Body text: %s", err, response.text
            )
        else:
            logging.error("Error parsing response JSON: %s. Response is None", err)
    return response_json


def parse_params(params=None, embeds=None):
    if params is None:
        params = {}
    if embeds is None:
        embeds = []
    if len(embeds) > 0:
        params["embed"] = ",".join(embeds)

    return params


class DSpaceClient:
    """
    Main class of the API client itself. This client uses request sessions to connect and
    authenticate to the REST API, maintain XSRF tokens, and all GET, POST, PUT, PATCH operations.
    Low-level api_get, api_post, api_put, api_delete, api_patch functions are defined to
    handle the requests and do retries / XSRF refreshes where necessary.
    Higher level get, create, update, partial_update (patch) functions are implemented
    for each DSO type
    """

    # Set up basic environment, variables
    session = None
    API_ENDPOINT = "http://localhost:8080/server/api"
    SOLR_ENDPOINT = "http://localhost:8983/solr"
    SOLR_AUTH = None
    USER_AGENT = f"DSpace-Python-REST-Client/{__version__}"
    if "DSPACE_API_ENDPOINT" in os.environ:
        API_ENDPOINT = os.environ["DSPACE_API_ENDPOINT"]
    LOGIN_URL = f"{API_ENDPOINT}/authn/login"
    USERNAME = "username@test.system.edu"
    if "DSPACE_API_USERNAME" in os.environ:
        USERNAME = os.environ["DSPACE_API_USERNAME"]
    PASSWORD = "password"
    if "DSPACE_API_PASSWORD" in os.environ:
        PASSWORD = os.environ["DSPACE_API_PASSWORD"]
    if "SOLR_ENDPOINT" in os.environ:
        SOLR_ENDPOINT = os.environ["SOLR_ENDPOINT"]
    if "SOLR_AUTH" in os.environ:
        SOLR_AUTH = os.environ["SOLR_AUTH"]
    if "USER_AGENT" in os.environ:
        USER_AGENT = os.environ["USER_AGENT"]
    verbose = False
    ITER_PAGE_SIZE = 20

    # Simple enum for patch operation types
    class PatchOperation:
        ADD = "add"
        REMOVE = "remove"
        REPLACE = "replace"
        MOVE = "move"

    def paginated(embed_name, item_constructor, embedding=lambda x: x):
        """
        @param embed_name: The key under '_embedded' in the JSON response that contains the
        resources to be paginated. (e.g. 'collections', 'objects', 'items', etc.)
        @param item_constructor: A callable that takes a resource dictionary and returns an item.
        @param embedding: Optional post-fetch processing lambda (default: identity function)
        for each resource
        @return: A decorator that, when applied to a method, follows pagination and yields
        each resource
        """

        def decorator(fun):
            @functools.wraps(fun)
            def decorated(self, *args, **kwargs):
                def do_paginate(url, params):
                    params["size"] = self.ITER_PAGE_SIZE

                    while url is not None:
                        r_json = embedding(self.fetch_resource(url, params))
                        for resource in r_json.get("_embedded", {}).get(embed_name, []):
                            yield item_constructor(resource)

                        if "next" in r_json.get("_links", {}):
                            url = r_json["_links"]["next"]["href"]
                            # assume the ‘next’ link contains all the
                            # params needed for the correct next page:
                            params = {}
                        else:
                            url = None

                return fun(do_paginate, self, *args, **kwargs)

            return decorated

        return decorator

    def __init__(
        self,
        api_endpoint=API_ENDPOINT,
        username=USERNAME,
        password=PASSWORD,
        solr_endpoint=SOLR_ENDPOINT,
        solr_auth=SOLR_AUTH,
        fake_user_agent=False,
    ):
        """
        Accept optional API endpoint, username, password arguments using the OS environment
        variables as defaults
        :param api_endpoint:    base path to DSpace REST API, eg. http://localhost:8080/server/api
        :param username:        username with appropriate privileges to perform operations on
                                REST API
        :param password:        password for the above username
        """
        self.session = requests.Session()
        self.API_ENDPOINT = api_endpoint
        self.LOGIN_URL = f"{self.API_ENDPOINT}/authn/login"
        self.USERNAME = username
        self.PASSWORD = password
        self.SOLR_ENDPOINT = solr_endpoint
        self.solr = pysolr.Solr(
            url=solr_endpoint, always_commit=True, timeout=300, auth=solr_auth
        )
        # If fake_user_agent was specified, use this string that is known (as of 2023-12-03) to succeed with
        # requests to Cloudfront-protected API endpoints (tested on demo.dspace.org)
        # Otherwise, the user agent will be the more helpful and accurate default of 'DSpace Python REST Client'
        # To override the user agent to your own string, instead set the USER_AGENT environment variable first
        # eg `export USER_AGENT="My Custom Agent String / 1.0`, and don't specify a value for fake_user_agent
        if fake_user_agent:
            self.USER_AGENT = (
                "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/39.0.2171.95 Safari/537.36"
            )
        # Set headers based on this
        self.auth_request_headers = {"User-Agent": self.USER_AGENT}
        self.request_headers = {
            "Content-type": "application/json",
            "User-Agent": self.USER_AGENT,
        }
        self.list_request_headers = {
            "Content-type": "text/uri-list",
            "User-Agent": self.USER_AGENT,
        }

    def authenticate(self, retry=False):
        """
        Authenticate with the DSpace REST API. As with other operations, perform XSRF refreshes when necessary.
        After POST, check /authn/status and log success if the authenticated json property is true
        @return: response object
        """
        # Set headers for requests made during authentication
        # Get and update CSRF token
        r = self.session.post(
            self.LOGIN_URL,
            data={"user": self.USERNAME, "password": self.PASSWORD},
            headers=self.auth_request_headers,
        )
        self.update_token(r)

        if r.status_code == 403:
            # 403 Forbidden
            # If we had a CSRF failure, retry the request with the updated token
            # After speaking in #dev it seems that these do need occasional refreshes but I suspect
            # it's happening too often for me, so check for accidentally triggering it
            if retry:
                logging.error(
                    "Too many retries updating token: %s: %s", r.status_code, r.text
                )
                return False
            else:
                logging.debug("Retrying request with updated CSRF token")
                return self.authenticate(retry=True)

        if r.status_code == 401:
            # 401 Unauthorized
            # If we get a 401, this means a general authentication failure
            logging.error(
                "Authentication failure: invalid credentials for user %s", self.USERNAME
            )
            return False

        # Update headers with new bearer token if present
        if "Authorization" in r.headers:
            self.session.headers.update(
                {"Authorization": r.headers.get("Authorization")}
            )

        # Get and check authentication status
        r = self.session.get(
            f"{self.API_ENDPOINT}/authn/status", headers=self.request_headers
        )
        if r.status_code == 200:
            r_json = parse_json(r)
            if "authenticated" in r_json and r_json["authenticated"] is True:
                logging.info("Authenticated successfully as %s", self.USERNAME)
                return r_json["authenticated"]

        # Default, return false
        return False

    def refresh_token(self):
        """
        If the DSPACE-XSRF-TOKEN appears, we need to update our local stored token and re-send our API request
        @return: None
        """
        r = self.api_post(self.LOGIN_URL, None, None)
        self.update_token(r)

    def api_get(self, url, params=None, data=None, headers=None):
        """
        Perform a GET request. Refresh XSRF token if necessary.
        @param url:     DSpace REST API URL
        @param params:  any parameters to include (eg ?page=0)
        @param data:    any data to supply (typically not relevant for GET)
        @param headers: any override headers (eg. with short-lived token for download)
        @return:        Response from API
        """
        if headers is None:
            headers = self.request_headers
        r = self.session.get(url, params=params, data=data, headers=headers)
        self.update_token(r)
        return r

    def api_post(self, url, params, json, retry=False):
        """
        Perform a POST request. Refresh XSRF token if necessary.
        POSTs are typically used to create objects.
        @param url:     DSpace REST API URL
        @param params:  Any parameters to include (eg ?parent=abbc-....)
        @param json:    Data in json-ready form (dict) to send as POST body (eg. item.as_dict())
        @param retry:   Has this method already been retried? Used if we need to refresh XSRF.
        @return:        Response from API
        """
        r = self.session.post(
            url, json=json, params=params, headers=self.request_headers
        )
        self.update_token(r)

        if r.status_code == 403:
            # 403 Forbidden
            # If we had a CSRF failure, retry the request with the updated token
            # After speaking in #dev it seems that these do need occasional refreshes but I suspect
            # it's happening too often for me, so check for accidentally triggering it
            r_json = parse_json(r)
            if "message" in r_json and "CSRF token" in r_json["message"]:
                if retry:
                    logging.warning(
                        "Too many retries updating token: %s: %s", r.status_code, r.text
                    )
                else:
                    logging.debug("Retrying request with updated CSRF token")
                    return self.api_post(url, params=params, json=json, retry=True)

        return r

    def api_post_uri(self, url, params, uri_list, retry=False):
        """
        Perform a POST request. Refresh XSRF token if necessary.
        POSTs are typically used to create objects.
        @param url:     DSpace REST API URL
        @param params:  Any parameters to include (eg ?parent=abbc-....)
        @param uri_list: One or more URIs referencing objects
        @param retry:   Has this method already been retried? Used if we need to refresh XSRF.
        @return:        Response from API
        """
        r = self.session.post(
            url, data=uri_list, params=params, headers=self.list_request_headers
        )
        self.update_token(r)

        if r.status_code == 403:
            # 403 Forbidden
            # If we had a CSRF failure, retry the request with the updated token
            # After speaking in #dev it seems that these do need occasional refreshes but I suspect
            # it's happening too often for me, so check for accidentally triggering it
            r_json = r.json()
            if "message" in r_json and "CSRF token" in r_json["message"]:
                if retry:
                    logging.warning(
                        "Too many retries updating token: %s: %s", r.status_code, r.text
                    )
                else:
                    logging.debug("Retrying request with updated CSRF token")
                    return self.api_post_uri(
                        url, params=params, uri_list=uri_list, retry=True
                    )

        return r

    def api_put(self, url, params, json, retry=False):
        """
        Perform a PUT request. Refresh XSRF token if necessary.
        PUTs are typically used to update objects.
        @param url:     DSpace REST API URL
        @param params:  Any parameters to include (eg ?parent=abbc-....)
        @param json:    Data in json-ready form (dict) to send as PUT body (eg. item.as_dict())
        @param retry:   Has this method already been retried? Used if we need to refresh XSRF.
        @return:        Response from API
        """
        r = self.session.put(
            url, params=params, json=json, headers=self.request_headers
        )
        self.update_token(r)

        if r.status_code == 403:
            # 403 Forbidden
            # If we had a CSRF failure, retry the request with the updated token
            # After speaking in #dev it seems that these do need occasional refreshes but I suspect
            # it's happening too often for me, so check for accidentally triggering it
            logging.debug(r.text)
            # Parse response
            r_json = parse_json(r)
            if "message" in r_json and "CSRF token" in r_json["message"]:
                if retry:
                    logging.warning(
                        "Too many retries updating token: %s: %s", r.status_code, r.text
                    )
                else:
                    logging.debug("Retrying request with updated CSRF token")
                    return self.api_put(url, params=params, json=json, retry=True)

        return r

    def api_delete(self, url, params, retry=False):
        """
        Perform a DELETE request. Refresh XSRF token if necessary.
        DELETES are typically used to update objects.
        @param url:     DSpace REST API URL
        @param params:  Any parameters to include (eg ?parent=abbc-....)
        @param retry:   Has this method already been retried? Used if we need to refresh XSRF.
        @return:        Response from API
        """
        r = self.session.delete(url, params=params, headers=self.request_headers)
        self.update_token(r)

        if r.status_code == 403:
            # 403 Forbidden
            # If we had a CSRF failure, retry the request with the updated token
            # After speaking in #dev it seems that these do need occasional refreshes but I suspect
            # it's happening too often for me, so check for accidentally triggering it
            logging.debug(r.text)
            # Parse response
            r_json = parse_json(r)
            if "message" in r_json and "CSRF token" in r_json["message"]:
                if retry:
                    logging.warning(
                        "Too many retries updating token: %s: %s", r.status_code, r.text
                    )
                else:
                    logging.debug("Retrying request with updated CSRF token")
                    return self.api_delete(url, params=params, retry=True)

        return r

    def api_patch(self, url, operation, path, value, params=None, retry=False):
        """
        @param url: DSpace REST API URL
        @param operation: 'add', 'remove', 'replace', or 'move' (see PatchOperation enumeration)
        @param path: path to perform operation - eg, metadata, withdrawn, etc.
        @param value: new value for add or replace operations, or 'original' path for move operations
        @param retry:   Has this method already been retried? Used if we need to refresh XSRF.
        @return:
        @see https://github.com/DSpace/RestContract/blob/main/metadata-patch.md
        """
        if url is None:
            logging.error("Missing required URL argument")
            return None
        if path is None:
            logging.error(
                "Need valid path eg. /withdrawn or /metadata/dc.title/0/language"
            )
            return None
        if (
            operation == self.PatchOperation.ADD
            or operation == self.PatchOperation.REPLACE
            or operation == self.PatchOperation.MOVE
        ) and value is None:
            # missing value required for add/replace/move operations
            logging.error(
                'Missing required "value" argument for add/replace/move operations'
            )
            return None

        # compile patch data
        data = {"op": operation, "path": path}
        if value is not None:
            if operation == self.PatchOperation.MOVE:
                data["from"] = value
            else:
                data["value"] = value

        # set headers
        # perform patch request
        r = self.session.patch(
            url, json=[data], headers=self.request_headers, params=params
        )
        self.update_token(r)

        if r.status_code == 403:
            # 403 Forbidden
            # If we had a CSRF failure, retry the request with the updated token
            # After speaking in #dev it seems that these do need occasional refreshes but I suspect
            # it's happening too often for me, so check for accidentally triggering it
            logging.debug(r.text)
            r_json = parse_json(r)
            if "message" in r_json and "CSRF token" in r_json["message"]:
                if retry:
                    logging.warning(
                        "Too many retries updating token: %s: %s", r.status_code, r.text
                    )
                else:
                    logging.debug("Retrying request with updated CSRF token")
                    return self.api_patch(url, operation, path, value, params, True)
        elif r.status_code == 200:
            # 200 Success
            logging.info(
                "successful patch update to %s %s", r.json()["type"], r.json()["id"]
            )

        # Return the raw API response
        return r

    # PAGINATION
    def search_objects(
        self,
        query=None,
        scope=None,
        filters=None,
        page=0,
        size=20,
        sort=None,
        dso_type=None,
        configuration='default',
        embeds=None,
    ):
        """
        Do a basic search with optional query, filters and dsoType params.
        @param query:   query string
        @param scope:   uuid to limit search scope, eg. owning collection, parent community, etc.
        @param filters: discovery filters as dict eg. {'f.entityType': 'Publication,equals', ... }
        @param page: page number (not like 'start' as this is not row number, but page number of size {size})
        @param size: size of page (aka. 'rows'), affects the page parameter above
        @param sort: sort eg. 'title,asc'
        @param dso_type: DSO type to further filter results
        @param configuration: Search (discovery) configuration to apply to the query
        @param embeds:  Optional list of embeds to apply to each search object result
        @return:        list of DspaceObject objects constructed from API resources
        """
        dsos = []
        if filters is None:
            filters = {}
        url = f"{self.API_ENDPOINT}/discover/search/objects"
        params = parse_params(embeds=embeds)
        if query is not None:
            params["query"] = query
        if scope is not None:
            params["scope"] = scope
        if dso_type is not None:
            params["dsoType"] = dso_type
        if size is not None:
            params["size"] = size
        if page is not None:
            params["page"] = page
        if sort is not None:
            params["sort"] = sort
        if configuration is not None:
            params['configuration'] = configuration

        r_json = self.fetch_resource(url=url, params={**params, **filters})

        # instead lots of 'does this key exist, etc etc' checks, just go for it and wrap in a try?
        try:
            results = r_json["_embedded"]["searchResult"]["_embedded"]["objects"]
            for result in results:
                resource = result["_embedded"]["indexableObject"]
                dso = SimpleDSpaceObject(resource)
                dsos.append(dso)
        except (TypeError, ValueError) as err:
            logging.error("error parsing search result json %s", err)

        return dsos

    @paginated(
        embed_name="objects",
        item_constructor=lambda x: SimpleDSpaceObject(
            x["_embedded"]["indexableObject"]
        ),
        embedding=lambda x: x["_embedded"]["searchResult"],
    )
    def search_objects_iter(
        do_paginate,
        self,
        query=None,
        scope=None,
        filters=None,
        dso_type=None,
        sort=None,
        configuration='default',
        embeds=None,
    ):
        """
        Do a basic search as in search_objects, automatically handling pagination by requesting the next page when all items from one page have been consumed
        @param query:   query string
        @param scope:   uuid to limit search scope, eg. owning collection, parent community, etc.
        @param filters: discovery filters as dict eg. {'f.entityType': 'Publication,equals', ... }
        @param sort: sort eg. 'title,asc'
        @param dso_type: DSO type to further filter results
        @param configuration: Search (discovery) configuration to apply to the query
        @param embeds:  Optional list of embeds to apply to each search object result
        @return:        Iterator of SimpleDSpaceObject
        """
        if filters is None:
            filters = {}
        url = f"{self.API_ENDPOINT}/discover/search/objects"
        params = parse_params(embeds=embeds)
        if query is not None:
            params["query"] = query
        if scope is not None:
            params["scope"] = scope
        if dso_type is not None:
            params["dsoType"] = dso_type
        if sort is not None:
            params["sort"] = sort
        if configuration is not None:
            params['configuration'] = configuration

        return do_paginate(url, {**params, **filters})

    def fetch_resource(self, url, params=None):
        """
        Simple function for higher-level 'get' functions to use whenever they want
        to retrieve JSON resources from the API
        @param url:     DSpace REST API URL
        @param params:  Optional params
        @return:        JSON parsed from API response or None if error
        """
        r = self.api_get(url, params, None)
        if r.status_code != 200:
            logging.error("Error encountered fetching resource: %s", r.text)
            return None
        # ValueError / JSON handling moved to static method
        return parse_json(r)

    def get_dso(self, url, uuid, params=None, embeds=None):
        """
        Base 'get DSpace Object' function.
        Uses fetch_resource which itself calls parse_json on the raw response before returning.
        @param url:     DSpace REST API URL
        @param uuid:    UUID of object to retrieve
        @param params:  Optional params
        @param embeds:  Optional list of embeds to include in the request
        @return:        Parsed JSON response from fetch_resource
        """
        try:
            # Try to get UUID version to test validity
            id = UUID(uuid).version
            url = f"{url}/{uuid}"
            params = parse_params(params, embeds=embeds)
            return self.api_get(url, params, None)
        except ValueError:
            logging.error("Invalid DSO UUID: %s", uuid)
            return None

    def create_dso(self, url, params, data, embeds=None):
        """
        Base 'create DSpace Object' function.
        Takes JSON data and some POST parameters and returns the response.
        @param url:     DSpace REST API URL
        @param params:  Any parameters to pass in the request, eg. parentCollection for a new item
        @param data:    JSON data expected by the REST API to create the new resource
        @param embeds:  Optional list of embeds (embed linked resources in response JSON)
        @return:        Raw API response. New DSO *could* be returned but for error checking purposes, raw response
                        is nice too and can always be parsed from this response later.
        """
        r = self.api_post(url, parse_params(params, embeds), data)
        if r.status_code == 201:
            # 201 Created - success!
            new_dso = parse_json(r)
            logging.info(
                "%s %s created successfully!", new_dso["type"], new_dso["uuid"]
            )
        else:
            logging.error(
                "create operation failed: %s: %s (%s)", r.status_code, r.text, url
            )
        return r

    def update_dso(self, dso, params=None, embeds=None):
        """
        Update DSpaceObject. Takes a DSpaceObject and any optional parameters. Will send a PUT update to the remote
        object and return the updated object, typed correctly.
        :param dso:     DSpaceObject with locally updated data, to send in PUT request
        :param params:  Optional parameters
        :return:

        """
        if dso is None:
            return None
        dso_type = type(dso)
        if not isinstance(dso, SimpleDSpaceObject):
            logging.error(
                "Only SimpleDSpaceObject types (eg Item, Collection, Community) "
                "are supported by generic update_dso PUT."
            )
            return dso
        try:
            # Get self URI from HAL links
            url = dso.links["self"]["href"]
            # Get and clean data - there are some unalterable fields that could cause errors
            data = dso.as_dict()
            if "lastModified" in data:
                data.pop("lastModified")
            # Parse parameters
            params = parse_params(params, embeds)

            r = self.api_put(url, params=params, json=data)
            if r.status_code == 200:
                # 200 OK - success!
                updated_dso = dso_type(parse_json(r))
                logging.info(
                    "%s %s updated successfully!", updated_dso.type, updated_dso.uuid
                )
                return updated_dso
            else:
                logging.error(
                    "update operation failed: %s: %s (%s)", r.status_code, r.text, url
                )
                return None

        except ValueError:
            logging.error("Error parsing DSO response", exc_info=True)
            return None

    def delete_dso(self, dso=None, url=None, params=None):
        """
        Delete DSpaceObject. Takes a DSpaceObject and any optional parameters. Will send a PUT update to the remote
        object and return the updated object, typed correctly.
        :param dso:     DSpaceObject from which to parse self link
        :param params:  Optional parameters
        :param url:     URI if not deleting from DSO
        :return:

        """
        if dso is None:
            if url is None:
                logging.error("Need a DSO or a URL to delete")
                return None
        else:
            if not isinstance(dso, SimpleDSpaceObject):
                logging.error(
                    "Only SimpleDSpaceObject types (eg Item, Collection, Community, EPerson) "
                    "are supported by generic update_dso PUT."
                )
                return dso
            # Get self URI from HAL links
            url = dso.links["self"]["href"]

        try:
            r = self.api_delete(url, params=params)
            if r.status_code == 204:
                # 204 No Content - success!
                logging.info("%s was deleted successfully!", url)
                return r
            else:
                logging.error(
                    "update operation failed: %s: %s (%s)", r.status_code, r.text, url
                )
                return None
        except ValueError as e:
            logging.error("Error deleting DSO %s: %s", dso.uuid, e)
            return None

    # PAGINATION
    def get_bundles(
        self, parent=None, uuid=None, page=0, size=20, sort=None, embeds=None
    ):
        """
        Get bundles for an item
        @param parent:  python Item object, from which the UUID will be referenced in the URL.
                        This is mutually exclusive to the 'uuid' argument, returning all bundles for the item.
        @param uuid:    Bundle UUID. This is mutually exclusive to the 'parent' argument, returning just this bundle
        @param embeds:  Optional list of resources to embed in response JSON
        @return:        List of bundles (single UUID bundle result is wrapped in a list before returning)
        """
        # TODO: It is probably wise to allow the parent UUID to be simply passed as an alternative to having the full
        #  python object as constructed by this REST client, for more flexible usage.
        bundles = []
        single_result = False
        if uuid is not None:
            url = f"{self.API_ENDPOINT}/core/bundles/{uuid}"
            single_result = True
        elif parent is not None:
            url = f"{self.API_ENDPOINT}/core/items/{parent.uuid}/bundles"
        else:
            return []
        params = parse_params(embeds=embeds)
        if size is not None:
            params["size"] = size
        if page is not None:
            params["page"] = page
        if sort is not None:
            params["sort"] = sort
        r_json = self.fetch_resource(url, params=params)
        try:
            if single_result:
                bundles.append(Bundle(r_json))
            if not single_result:
                resources = r_json["_embedded"]["bundles"]
                for resource in resources:
                    bundles.append(Bundle(resource))
        except ValueError as err:
            logging.error("error parsing bundle results: %s", err)

        return bundles

    @paginated("bundles", Bundle)
    def get_bundles_iter(do_paginate, self, parent, sort=None, embeds=None):
        """
        Get bundles for an item, automatically handling pagination by requesting the next page when all items from one page have been consumed
        @param parent:  python Item object, from which the UUID will be referenced in the URL.
        @param embeds:  Optional list of resources to embed in response JSON
        @return:        Iterator of Bundle
        """
        url = f"{self.API_ENDPOINT}/core/items/{parent.uuid}/bundles"
        params = parse_params(embeds=embeds)
        if sort is not None:
            params["sort"] = sort

        return do_paginate(url, params)

    def create_bundle(self, parent=None, name="ORIGINAL", embeds=None):
        """
        Create new bundle in the specified item
        @param parent:  Parent python Item, the UUID of which will be used in the URL path
        @param name:    Name of the bundle. Default: ORIGINAL
        @param embeds:  Optional list of resources to embed in response JSON
        @return:        constructed python Bundle object from the response JSON
                        (note: this is a bit inconsistent with create_dso usage where the raw response is returned)
        """
        # TODO: It is probably wise to allow the parent UUID to be simply passed as an alternative to having the full
        #  python object as constructed by this REST client, for more flexible usage.
        if parent is None:
            return None
        url = f"{self.API_ENDPOINT}/core/items/{parent.uuid}/bundles"
        return Bundle(
            api_resource=parse_json(
                self.api_post(
                    url,
                    params=parse_params(embeds=embeds),
                    json={"name": name, "metadata": {}},
                )
            )
        )

    # PAGINATION
    def get_bitstreams(
        self, uuid=None, bundle=None, page=0, size=20, sort=None, embeds=None
    ):
        """
        Get a specific bitstream UUID, or all bitstreams for a specific bundle
        @param uuid:    UUID of a specific bitstream to retrieve
        @param bundle:  A python Bundle object to parse for bitstream links to retrieve
        @param page:    Page number, for pagination over large result sets (default: 0)
        @param size:    Size of results per page (default: 20)
        @param embeds:  Optional list of resources to embed in response JSON
        @return:        list of python Bitstream objects
        """
        url = f"{self.API_ENDPOINT}/core/bitstreams/{uuid}"
        if uuid is None and bundle is None:
            return []
        if uuid is None and isinstance(bundle, Bundle):
            if "bitstreams" in bundle.links:
                url = bundle.links["bitstreams"]["href"]
            else:
                if bundle is None:
                    logging.error("Bundle cannot be None")
                    return []
                if bundle is None:
                    logging.error("Bundle cannot be None")
                    return []
                url = f"{self.API_ENDPOINT}/core/bundles/{bundle.uuid}/bitstreams"
                logging.warning(
                    "Cannot find bundle bitstream links, will try to construct manually: %s",
                    url,
                )
        # Perform the actual request. By now, our URL and parameter should be properly set
        params = parse_params(embeds=embeds)
        if size is not None:
            params["size"] = size
        if page is not None:
            params["page"] = page
        if sort is not None:
            params["sort"] = sort

        r_json = self.fetch_resource(url, params=params)
        if "_embedded" in r_json:
            if "bitstreams" in r_json["_embedded"]:
                bitstreams = []
                for bitstream_resource in r_json["_embedded"]["bitstreams"]:
                    bitstream = Bitstream(bitstream_resource)
                    bitstreams.append(bitstream)
                return bitstreams

    @paginated("bitstreams", Bitstream)
    def get_bitstreams_iter(do_paginate, self, bundle, sort=None, embeds=None):
        """
        Get all bitstreams for a specific bundle, automatically handling pagination by requesting the next page when all items from one page have been consumed
        @param bundle:  A python Bundle object to parse for bitstream links to retrieve
        @param embeds:  Optional list of resources to embed in response JSON
        @return:        Iterator of Bitstream
        """
        if "bitstreams" in bundle.links:
            url = bundle.links["bitstreams"]["href"]
        else:
            url = f"{self.API_ENDPOINT}/core/bundles/{bundle.uuid}/bitstreams"
            logging.warning(
                "Cannot find bundle bitstream links, will try to construct manually: %s",
                url,
            )
        params = parse_params(embeds=embeds)
        if sort is not None:
            params["sort"] = sort

        return do_paginate(url, params)

    def create_bitstream(
        self,
        bundle=None,
        name=None,
        path=None,
        mime=None,
        metadata=None,
        embeds=None,
        retry=False,
    ):
        """
        Upload a file and create a bitstream for a specified parent bundle, from the uploaded file and
        the supplied metadata.
        This create method is a bit different to the others, it does not use create_dso or the api_post lower level
        methods, instead it has to use a prepared session POST request which will allow the multi-part upload to work
        successfully with the correct byte size and persist the session data.
        This is also why it directly implements the 'retry' functionality instead of relying on api_post.
        @param bundle:      python Bundle object
        @param name:        Bitstream name
        @param path:        Local filesystem path to the file that will be uploaded
        @param mime:        MIME string of the uploaded file
        @param metadata:    Full metadata JSON
        @param retry:       A 'retried' indicator. If the first attempt fails due to an expired or missing auth
                            token, the request will retry once, after the token is refreshed. (default: False)
        @return:            constructed Bitstream object from the API response, or None if the operation failed.
        """
        # TODO: It is probably wise to allow the bundle UUID to be simply passed as an alternative to having the full
        #  python object as constructed by this REST client, for more flexible usage.
        # TODO: Better error detection and handling for file reading
        if metadata is None:
            metadata = {}
        url = f"{self.API_ENDPOINT}/core/bundles/{bundle.uuid}/bitstreams"
        file = (name, open(path, "rb"), mime)
        files = {"file": file}
        properties = {"name": name, "metadata": metadata, "bundleName": bundle.name}
        payload = {"properties": json.dumps(properties) + ";application/json"}
        h = self.session.headers
        h.update({"Content-Encoding": "gzip", "User-Agent": self.USER_AGENT})
        req = Request(
            "POST",
            url,
            data=payload,
            headers=h,
            files=files,
            params=parse_params(embeds=embeds),
        )
        prepared_req = self.session.prepare_request(req)
        r = self.session.send(prepared_req)
        if "DSPACE-XSRF-TOKEN" in r.headers:
            t = r.headers["DSPACE-XSRF-TOKEN"]
            logging.debug("Updating token to %s", t)
            self.session.headers.update({"X-XSRF-Token": t})
            self.session.cookies.update({"X-XSRF-Token": t})
        if r.status_code == 403:
            r_json = parse_json(r)
            if "message" in r_json and "CSRF token" in r_json["message"]:
                if retry:
                    logging.error("Already retried... something must be wrong")
                else:
                    logging.debug("Retrying request with updated CSRF token")
                    return self.create_bitstream(
                        bundle, name, path, mime, metadata, embeds, True
                    )

        if r.status_code == 201 or r.status_code == 200:
            # Success
            return Bitstream(api_resource=parse_json(r))
        else:
            logging.error("Error creating bitstream: %s: %s", r.status_code, r.text)
            return None

    def download_bitstream(self, uuid=None):
        """
        Download bitstream and return full response object including headers, and content
        @param uuid:
        @return: full response object including headers, and content
        """
        url = f"{self.API_ENDPOINT}/core/bitstreams/{uuid}/content"
        h = {
            "User-Agent": self.USER_AGENT,
            "Authorization": self.get_short_lived_token(),
        }
        r = self.api_get(url, headers=h)
        if r.status_code == 200:
            return r

    # PAGINATION
    def get_communities(
        self, uuid=None, page=0, size=20, sort=None, top=False, embeds=None
    ):
        """
        Get communities - either all, for single UUID, or all top-level (ie no sub-communities)
        @param uuid:    string UUID if getting single community
        @param page:    integer page (default: 0)
        @param size:    integer size (default: 20)
        @param top:     whether to restrict search to top communities (default: false)
        @param embeds:  list of resources to embed in response JSON
        @return:        list of communities, or None if error
        """
        url = f"{self.API_ENDPOINT}/core/communities"
        params = parse_params(embeds=embeds)
        if size is not None:
            params["size"] = size
        if page is not None:
            params["page"] = page
        if sort is not None:
            params["sort"] = sort
        if uuid is not None:
            try:
                # This isn't used, but it'll throw a ValueError if not a valid UUID
                id = UUID(uuid).version
                # Set URL and parameters
                url = f"{url}/{uuid}"
                params = None
            except ValueError:
                logging.error("Invalid community UUID: %s", uuid)
                return None

        if top:
            # Set new URL
            url = f"{url}/search/top"

        logging.debug("Performing get on %s", url)
        # Perform actual get
        r_json = self.fetch_resource(url, params)
        # Empty list
        communities = []
        if "_embedded" in r_json:
            if "communities" in r_json["_embedded"]:
                for community_resource in r_json["_embedded"]["communities"]:
                    communities.append(Community(community_resource))
        elif "uuid" in r_json:
            # This is a single communities
            communities.append(Community(r_json))
        # Return list (populated or empty)
        return communities

    @paginated("communities", Community)
    def get_communities_iter(do_paginate, self, sort=None, top=False, embeds=None):
        """
        Get communities as an iterator, automatically handling pagination by requesting the next page when all items from one page have been consumed
        @param top:     whether to restrict search to top communities (default: false)
        @param embeds:  list of resources to embed in response JSON
        @return: Iterator of Community
        """
        if top:
            url = f"{self.API_ENDPOINT}/core/communities/search/top"
        else:
            url = f"{self.API_ENDPOINT}/core/communities"

        params = parse_params(embeds=embeds)
        if sort is not None:
            params["sort"] = sort

        return do_paginate(url, params)

    def create_community(self, parent, data, embeds=None):
        """
        Create a community, either top-level or beneath a given parent
        @param parent:  (optional) parent UUID to pass as a parameter to create_dso
        @param data:    Full JSON data for the new community
        @param embeds:  Optional list of resources to embed in response JSON
        @return:        python Community object constructed from the API response
        """
        # TODO: To be consistent with other create methods, this should probably also allow a Community object
        #  to be passed instead of just the UUID as a string
        url = f"{self.API_ENDPOINT}/core/communities"
        params = parse_params(embeds=embeds)
        if parent is not None:
            params = {"parent": parent}
        return Community(api_resource=parse_json(self.create_dso(url, params, data)))

    def get_collections(
        self, uuid=None, community=None, page=0, size=20, sort=None, embeds=None
    ):
        """
        Get collections - all, or single UUID, or for a specific community
        @param uuid:        UUID string. If present, just a single collection is returned (overrides community arg)
        @param community:   Community object. If present (and no uuid present), collections for a community
        @param page:        Integer for page / offset of results. Default: 0
        @param size:        Integer for page size. Default: 20 (same as REST API default)
        @param embeds:      Optional list of resources to embed in response JSON
        @return:            list of Collection objects, or None if there was an error
                            for consistency of handling results, even the uuid search will be a list of one
        """
        url = f"{self.API_ENDPOINT}/core/collections"
        params = parse_params(embeds=embeds)
        if size is not None:
            params["size"] = size
        if page is not None:
            params["page"] = page
        if sort is not None:
            params["sort"] = sort
        # First, handle case of UUID. It overrides the other arguments as it is a request for a single collection
        if uuid is not None:
            try:
                id = UUID(uuid).version
                # Update URL and parameters
                url = f"{url}/{uuid}"
                params = None
            except ValueError:
                logging.error("Invalid collection UUID: %s", uuid)
                return None

        if community is not None:
            if (
                "collections" in community.links
                and "href" in community.links["collections"]
            ):
                # Update URL
                url = community.links["collections"]["href"]

        # Perform the actual request. By now, our URL and parameter should be properly set
        r_json = self.fetch_resource(url, params=params)
        # Empty list
        collections = []
        if "_embedded" in r_json:
            # This is a list of collections
            if "collections" in r_json["_embedded"]:
                for collection_resource in r_json["_embedded"]["collections"]:
                    collections.append(Collection(collection_resource))
        elif "uuid" in r_json:
            # This is a single collection
            collections.append(Collection(r_json))

        # Return list (populated or empty)
        return collections

    @paginated("collections", Collection)
    def get_collections_iter(do_paginate, self, community=None, sort=None, embeds=None):
        """
        Get collections as an iterator, automatically handling pagination by requesting the next page when all items from one page have been consumed
        @param community:   Community object. If present, collections for a community
        @return:            Iterator of Collection
        """
        url = f"{self.API_ENDPOINT}/core/collections"
        params = parse_params(embeds=embeds)
        if sort is not None:
            params["sort"] = sort

        if community is not None:
            if (
                "collections" in community.links
                and "href" in community.links["collections"]
            ):
                url = community.links["collections"]["href"]

        return do_paginate(url, params)

    def create_collection(self, parent, data, embeds=None):
        """
        Create collection beneath a given parent community.
        @param parent:  UUID of parent community to pass as a parameter to create_dso
        @param data:    Full JSON data for the new collection
        @param embed:   Optional list of resources to embed in response JSON
        @return:        python Collection object constructed from the API response
        """
        # TODO: To be consistent with other create methods, this should probably also allow a Community object
        #  to be passed instead of just the UUID as a string
        url = f"{self.API_ENDPOINT}/core/collections"
        params = parse_params(embeds=embeds)
        if parent is not None:
            params = {"parent": parent}
        return Collection(api_resource=parse_json(self.create_dso(url, params, data)))

    def get_item(self, uuid, embeds=None):
        """
        Get an item, given its UUID
        @param uuid:    the UUID of the item
        @param embeds:      Optional list of resources to embed in response JSON
        @return:        the raw API response
        """
        # TODO - return constructed Item object instead, handling errors here?
        url = f"{self.API_ENDPOINT}/core/items"
        try:
            id = UUID(uuid).version
            url = f"{url}/{uuid}"
            return self.api_get(url, parse_params(embeds=embeds), None)
        except ValueError:
            logging.error("Invalid item UUID: %s", uuid)
            return None

    def get_items(self, embeds=None):
        """
        Get all archived items for a logged-in administrator. Admin only! Usually you will want to
        use search or browse methods instead of this method
        @param embeds:  Optional list of resources to embed in response JSON
        @return: A list of items, or an error
        """
        url = f"{self.API_ENDPOINT}/core/items"
        # Empty item list
        items = []
        # Perform the actual request
        r_json = self.fetch_resource(url, params=parse_params(embeds=embeds))
        # Empty list
        items = []
        if "_embedded" in r_json:
            # This is a list of items
            if "items" in r_json["_embedded"]:
                for item_resource in r_json["_embedded"]["items"]:
                    items.append(Item(item_resource))
        elif "uuid" in r_json:
            # This is a single item
            items.append(Item(r_json))

        # Return list (populated or empty)
        return items

    def create_item(self, parent, item, embeds=None):
        """
        Create an item beneath the given parent collection
        @param parent:  UUID of parent collection to pass as a parameter to create_dso
        @param item:    python Item object containing all the data and links expected by the REST API
        @param embeds:  Optional list of resources to embed in response JSON
        @return:        Item object constructed from the API response
        """
        url = f"{self.API_ENDPOINT}/core/items"
        if parent is None:
            logging.error("Need a parent UUID!")
            return None
        params = parse_params({"owningCollection": parent}, embeds)
        if not isinstance(item, Item):
            logging.error("Need a valid item")
            return None
        return Item(
            api_resource=parse_json(
                self.create_dso(url, params=params, data=item.as_dict())
            )
        )

    def create_item_version(self, item_uuid, summary=None, embeds=None):
        """
        Create a new version of an existing item.
        @param item_uuid: UUID of the item to version
        @param summary: Optional summary text for the new version
        @return: JSON response containing the new version information or None if an error occurs
        """
        url = f"{self.API_ENDPOINT}/versioning/versions"
        params = parse_params(embeds=embeds)
        if summary is not None:
            params["summary"] = summary

        # Construct the item URI
        item_uri = f"{self.API_ENDPOINT}/core/items/{item_uuid}"

        # Send the POST request with Content-Type:text/uri-list
        response = self.api_post_uri(url, params=params, uri_list=item_uri)

        if response.status_code == 201:
            # 201 Created - Success
            new_version = parse_json(response)
            logging.info("Created new version for item %s", item_uuid)
            return new_version
        else:
            logging.error(
                "Error creating item version: %s %s",
                response.status_code,
                response.text,
            )

        return None

    def update_item(self, item, embeds=None):
        """
        Update item. The Item passed to this method contains all the data, identifiers, links necessary to
        perform the update to the API. Note this is a full update, not a patch / partial update operation.
        @param item: python Item object
        @param embeds:  Optional list of resources to embed in response JSON
        @return:
        """
        if not isinstance(item, Item):
            logging.error("Need a valid item")
            return None
        return self.update_dso(item, params=parse_params(embeds=embeds))

    def add_metadata(
        self,
        dso,
        field,
        value,
        language=None,
        authority=None,
        confidence=-1,
        place="",
        embeds=None,
    ):
        """
        Add metadata to a DSO using the api_patch method (PUT, with path and operation and value)
        @param dso: DSO to patch
        @param field: Metadata field to patch
        @param value: Metadata value to patch
        @param language: Optional language
        @param authority: Optional authority
        @param confidence: Optional confidence
        @param place: Optional place (metadata position)
        @param embeds:  Optional list of resources to embed in response JSON
        :return:
        """
        if (
            dso is None
            or field is None
            or value is None
            or not isinstance(dso, DSpaceObject)
        ):
            # TODO: separate these tests, and add better error handling
            logging.error("Invalid or missing DSpace object, field or value string")
            return self

        dso_type = type(dso)

        # Place can be 0+ integer, or a hyphen - meaning "last"
        path = f"/metadata/{field}/{place}"
        patch_value = {
            "value": value,
            "language": language,
            "authority": authority,
            "confidence": confidence,
        }

        url = dso.links["self"]["href"]

        r = self.api_patch(
            url=url,
            operation=self.PatchOperation.ADD,
            path=path,
            value=patch_value,
            params=parse_params(embeds=embeds),
        )

        return dso_type(api_resource=parse_json(r))

    def create_user(self, user, token=None, embeds=None):
        """
        Create a user
        @param user:    python User object or Python dict containing all the data and links expected by the REST API
        @param token:   Token if creating new user (optional) from the link in a registration email
        @embeds:  Optional list of resources to embed in response JSON
        @return:        User object constructed from the API response
        """
        url = f"{self.API_ENDPOINT}/eperson/epersons"
        data = user
        if isinstance(user, User):
            data = user.as_dict()
            # TODO: Validation. Note, at least here I will just allow a dict instead of the pointless cast<->cast
            # that you see for other DSO types - still figuring out the best way
        params = parse_params(embeds=embeds)
        if token is not None:
            params = {"token": token}
        return User(
            api_resource=parse_json(self.create_dso(url, params=params, data=data))
        )

    def delete_user(self, user):
        if not isinstance(user, User):
            logging.error("Must be a valid user")
            return None
        return self.delete_dso(user)

    # PAGINATION
    def get_users(self, page=0, size=20, sort=None, embeds=None):
        """
        Get a list of users (epersons) in the DSpace instance
        @param page: Integer for page / offset of results. Default: 0
        @param size: Integer for page size. Default: 20 (same as REST API default)
        @param embeds: Optional list of resources to embed in response JSON
        @return:     list of User objects
        """
        url = f"{self.API_ENDPOINT}/eperson/epersons"
        users = []
        params = parse_params(embeds=embeds)
        if page is not None:
            params["page"] = page
        if size is not None:
            params["size"] = size
        if page is not None:
            params["page"] = page
        if sort is not None:
            params["sort"] = sort
        r = self.api_get(url, params=params)
        r_json = parse_json(response=r)
        if "_embedded" in r_json:
            if "epersons" in r_json["_embedded"]:
                for user_resource in r_json["_embedded"]["epersons"]:
                    users.append(User(user_resource))
        return users

    @paginated("epersons", User)
    def get_users_iter(do_paginate, self, sort=None, embeds=None):
        """
        Get an iterator of users (epersons) in the DSpace instance, automatically handling pagination by requesting the next page when all items from one page have been consumed
        @param sort:     Optional sort parameter
        @param embeds:   Optional list of resources to embed in response JSON
        @return:     Iterator of User
        """
        url = f"{self.API_ENDPOINT}/eperson/epersons"
        params = parse_params(embeds=embeds)
        if sort is not None:
            params["sort"] = sort

        return do_paginate(url, params)
    
    def get_user_by_uuid(self, uuid, embeds=None):
        """
        Get a single user by UUID
        @param uuid: UUID of the user
        @param embeds: Optional list of resources to embed in response JSON
        @return: User object constructed from the API response or None if not found
        """
        url = f"{self.API_ENDPOINT}/eperson/epersons/{uuid}"
        params = parse_params(embeds=embeds)
        r = self.api_get(url, params=params)
        r_json = parse_json(response=r)
        return User(r_json) if r_json else None
    
    def search_user_by_email(self, email):
        """
        Search for a user by email
        @param email: User's email address
        @return: User object if found, None otherwise
        """
        url = f"{self.API_ENDPOINT}/eperson/epersons/search/byEmail"
        params = {"email": email}
        r = self.api_get(url, params=params)
        r_json = parse_json(response=r)
        return User(r_json) if r_json else None

    def search_users_by_metadata(self, query, embeds=None):
        """
        Search users by metadata
        @param query: Search query (UUID, name, email, etc.)
        @param embeds: Optional list of resources to embed in response JSON
        @return: List of User objects matching the query
        """
        url = f"{self.API_ENDPOINT}/eperson/epersons/search/byMetadata"
        params = parse_params({"query": query}, embeds=embeds)
        r = self.api_get(url, params=params)
        r_json = parse_json(response=r)
        users = []
        if "_embedded" in r_json and "epersons" in r_json["_embedded"]:
            users = [
                User(user_resource) for user_resource in r_json["_embedded"]["epersons"]
            ]
        return users
    
    def get_eperson_id_of_user(self):
        """
        Get the EPerson ID of the current user
        authn/status response includes the eperson link
        the uuid can be parsed from the eperson link and returned as text
        @return: String of the user id or None in case of an error
        """
        url = f"{self.API_ENDPOINT}/authn/status"
        try:
            r = self.api_get(url)
            r_json = parse_json(response=r)
            if "_links" in r_json:
                eperson_href = r_json["_links"]["eperson"]["href"]
                path = urlparse(eperson_href).path
                uuid = os.path.basename(path)
                return uuid
            else:
                logging.error("EPerson link not found in response.")
                return None
        except Exception as e:
            logging.error("Error retrieving EPerson ID: %s", e)
            return None
        
    def get_special_groups_of_user(self):
        """
        Get the special groups of a user
        authn/status/specialGroups
        @return: List of Group objects or None in case of an error
        """
        url = f"{self.API_ENDPOINT}/authn/status/specialGroups"
        try:
            r = self.api_get(url)
            r_json = parse_json(response=r)
            if "_embedded" in r_json and "specialGroups" in r_json["_embedded"]:
                groups = [
                    Group(group_resource)
                    for group_resource in r_json["_embedded"]["specialGroups"]
                ]
                return groups
            logging.error("Special groups not found in response.")
            return None
        except Exception as e:
            logging.error("Error retrieving special groups: %s", e)
            return None

    def get_groups_of_user(self, user_uuid):
        """
        Get groups of a user
        @param user_uuid: UUID of the user
        @return: List of Group objects
        """
        url = f"{self.API_ENDPOINT}/eperson/epersons/{user_uuid}/groups"
        r = self.api_get(url)
        r_json = parse_json(response=r)
        groups = []
        if "_embedded" in r_json and "groups" in r_json["_embedded"]:
            groups = [
                Group(group_resource)
                for group_resource in r_json["_embedded"]["groups"]
            ]
        return groups
    
    def search_users_not_in_group(self, group_uuid, query=None, embeds=None):
        """
        Search users not in a specific group
        @param group_uuid: UUID of the group
        @param query: Search query (UUID, name, email, etc.)
        @param embeds: Optional list of resources to embed in response JSON
        @return: List of User objects matching the query
        """
        url = f"{self.API_ENDPOINT}/eperson/epersons/search/isNotMemberOf"
        params = parse_params(params={"group": group_uuid, "query": query}, embeds=embeds)
        r = self.api_get(url, params=params)
        r_json = parse_json(response=r)
        users = []
        if "_embedded" in r_json and "epersons" in r_json["_embedded"]:
            users = [
                User(user_resource) for user_resource in r_json["_embedded"]["epersons"]
            ]
        return users
    
    def update_user_metadata(self, user_uuid, path, value, embeds=None):
        """
        Update user metadata
        @param user_uuid: UUID of the user
        @param metadata_updates: List of metadata updates in the PATCH format
        @return: Updated User object or None if the operation fails
        """
        url = f"{self.API_ENDPOINT}/eperson/epersons/{user_uuid}"
        r = self.api_patch(
            url=url,
            operation="replace",
            path=path,
            value=value,
            params=parse_params(embeds=embeds),
        )
        r_json = parse_json(response=r)
        return User(r_json) if r_json else None
    
    def change_user_password(self, user_uuid, current_password, new_password):
        """
        Change the password of a user
        @param user_uuid: UUID of the user
        @param current_password: Current password of the user
        @param new_password: New password for the user
        @return: Boolean indicating success or failure
        """
        # TODO: ensure this is only triggered when the user management is done in DSpace directly.
        # If the user management is done in an external system (e.g. LDAP), this method should not be used.
        url = f"{self.API_ENDPOINT}/eperson/epersons/{user_uuid}"
        r = self.api_patch(
            url,
            operation="add",
            path="/password",
            value={
                "new_password": new_password,
                "current_password": current_password,
            },
        )
        if r.status_code == 200:
            logging.info("Updated Password for user %s", user_uuid)
            return True
        if r.status_code == 422:
            logging.error(
                "Password does not respect the rules configured in the regular expression."
            )
            return False
        logging.error("An error occurred updating the password.")
        return False

    def create_group(self, group, embeds=None):
        """
        Create a group
        @param group:    python Group object or Python dict containing all the data and links expected by the REST API
        @param embeds:  Optional list of resources to embed in response JSON
        @return:         User object constructed from the API response
        """
        url = f"{self.API_ENDPOINT}/eperson/groups"
        data = group
        if isinstance(group, Group):
            data = group.as_dict()
            # TODO: Validation. Note, at least here I will just allow a dict instead of the pointless cast<->cast
            # that you see for other DSO types - still figuring out the best way
        return Group(
            api_resource=parse_json(
                self.create_dso(url, params=parse_params(embeds=embeds), data=data)
            )
        )

    def get_groups(self, page=0, size=20, embeds=None):
        """
        Fetch all groups
        @param page: Page number for pagination
        @param size: Number of results per page
        @param embeds: Optional list of resources to embed in response JSON
        @return: List of Group objects
        """
        url = f"{self.API_ENDPOINT}/eperson/groups"
        params = parse_params({"page": page, "size": size}, embeds=embeds)
        response = self.api_get(url, params=params)
        response_json = parse_json(response=response)
        groups = []

        if "_embedded" in response_json and "groups" in response_json["_embedded"]:
            for group_data in response_json["_embedded"]["groups"]:
                groups.append(Group(group_data))

        return groups
    
    def get_subgroups(self, parent_uuid, page=0, size=20):
        """
        Get all subgroups of a parent group
        @param parent_uuid: UUID of the parent group
        @param page: Page number for pagination
        @param size: Number of results per page
        @return: List of Group objects
        """
        url = f"{self.API_ENDPOINT}/eperson/groups/{parent_uuid}/subgroups"
        params = parse_params({"page": page, "size": size})
        response = self.api_get(url, params=params)
        response_json = parse_json(response=response)
        subgroups = []

        if "_embedded" in response_json and "groups" in response_json["_embedded"]:
            for group_data in response_json["_embedded"]["groups"]:
                subgroups.append(Group(group_data))

        return subgroups

    def add_subgroup(self, parent_uuid, child_uuid):
        """
        Add a subgroup to a parent group
        @param parent_uuid: UUID of the parent group
        @param child_uuid: UUID of the subgroup to add
        @return: Boolean indicating success or failure
        """
        url = f"{self.API_ENDPOINT}/eperson/groups/{parent_uuid}/subgroups"
        data = f"{self.API_ENDPOINT}/eperson/groups/{child_uuid}"
        response = self.api_post_uri(url, uri_list=data, params=None)
        if response.status_code == 204:
            return True
        if response.status_code == 401:
            logging.error("You are not authenticated")
            return False
        if response.status_code == 403:
            logging.error("You are not logged in with sufficient permissions")
            return False
        if response.status_code == 404:
            logging.error("The parent group doesn't exist")
            return False
        if response.status_code == 422:
            logging.error(
                "The specified group is not found, or if adding the group would create a cyclic reference"
            )
            return False
        logging.error(
            "Failed to add subgroup %s to group %s: %s",
            child_uuid,
            parent_uuid,
            response.text,
        )
        return False

    def remove_subgroup(self, parent_uuid, child_uuid):
        """
        Remove a subgroup from a parent group
        @param parent_uuid: UUID of the parent group
        @param child_uuid: UUID of the subgroup to remove
        @return: Boolean indicating success or failure
        """
        url = f"{self.API_ENDPOINT}/eperson/groups/{parent_uuid}/subgroups/{child_uuid}"
        response = self.api_delete(url, params=None)
        if response.status_code == 204:
            return True
        if response.status_code == 401:
            logging.error("You are not authenticated")
            return False
        if response.status_code == 403:
            logging.error("You are not logged in with sufficient permissions")
            return False
        if response.status_code == 404:
            logging.error("The parent group doesn't exist")
            return False
        if response.status_code == 422:
            logging.error("The specified group is not found")
            return False
        logging.error(
            "Failed to remove subgroup %s from group %s: %s",
            child_uuid,
            parent_uuid,
            response.text,
        )
        return False
        
    def search_groups_by_metadata(self, query, page=0, size=20):
        """
        Search for groups by metadata
        @param query: Search query (UUID or group name)
        @param page: Page number for pagination
        @param size: Number of results per page
        @return: List of Group objects
        """
        url = f"{self.API_ENDPOINT}/eperson/groups/search/byMetadata"
        params = parse_params({"query": query, "page": page, "size": size})
        response = self.api_get(url, params=params)
        response_json = parse_json(response=response)
        groups = []

        if "_embedded" in response_json and "groups" in response_json["_embedded"]:
            for group_data in response_json["_embedded"]["groups"]:
                groups.append(Group(group_data))

        return groups
    
    def get_epersons_in_group(self, group_uuid, page=0, size=20):
        """
        Fetch all EPersons in a group
        @param group_uuid: UUID of the group
        @param page: Page number for pagination
        @param size: Number of results per page
        @return: List of User objects
        """
        url = f"{self.API_ENDPOINT}/eperson/groups/{group_uuid}/epersons"
        params = parse_params({"page": page, "size": size})
        response = self.api_get(url, params=params)
        response_json = parse_json(response=response)
        epersons = []

        if "_embedded" in response_json and "epersons" in response_json["_embedded"]:
            for eperson_data in response_json["_embedded"]["epersons"]:
                epersons.append(User(eperson_data))

        return epersons
    
    def add_eperson_to_group(self, group_uuid, eperson_uuid):
        """
        Add an EPerson to a group
        @param group_uuid: UUID of the group
        @param eperson_uuid: UUID of the EPerson to add
        @return: Boolean indicating success or failure
        """
        url = f"{self.API_ENDPOINT}/eperson/groups/{group_uuid}/epersons"
        # check if the eperson exists and is valid
        eperson = self.get_user_by_uuid(eperson_uuid)
        if eperson is None:
            logging.error("The specified EPerson does not exist")
            return False
        if not isinstance(eperson, User):
            logging.error("Invalid EPerson object")
            return False
        # check if the group exists and is valid
        group = self.get_group_by_uuid(group_uuid)
        if group is None:
            logging.error("The specified group does not exist")
            return False
        if not isinstance(group, Group):
            logging.error("Invalid Group object")
            return False
        data = f"{self.API_ENDPOINT}/eperson/epersons/{eperson_uuid}"
        response = self.api_post_uri(url, uri_list=data, params=None)
        if response.status_code == 204:
            return True
        if response.status_code == 401:
            logging.error("You are not authenticated")
            return False
        if response.status_code == 403:
            logging.error("You are not logged in with sufficient permissions")
            return False
        if response.status_code == 422:
            logging.error("The specified group or EPerson is not found")
            return False
        logging.error(
            "Failed to add EPerson %s to group %s: %s",
            eperson_uuid,
            group_uuid,
            response.text,
        )
        return False

    def remove_eperson_from_group(self, group_uuid, eperson_uuid):
        """
        Remove an EPerson from a group
        @param group_uuid: UUID of the group
        @param eperson_uuid: UUID of the EPerson to remove
        @return: Boolean indicating success or failure
        """
        url = f"{self.API_ENDPOINT}/eperson/groups/{group_uuid}/epersons/{eperson_uuid}"
        response = self.api_delete(url, params=None)
        if response.status_code == 204:
            return True
        if response.status_code == 401:
            logging.error("You are not authenticated")
            return False
        if response.status_code == 403:
            logging.error("You are not logged in with sufficient permissions")
            return False
        if response.status_code == 422:
            logging.error("The specified group or EPerson is not found")
            return False
        logging.error(
            "Failed to remove EPerson %s from group %s: %s",
            eperson_uuid,
            group_uuid,
            response.text,
        )
        return False
    
    def update_group_name(self, uuid, new_name):
        """
        Update the name of a group
        @param uuid: UUID of the group
        @param new_name: New name for the group
        @return: Updated Group object or None if the update fails
        """
        url = f"{self.API_ENDPOINT}/eperson/groups/{uuid}"
        response = self.api_patch(
            url, operation="replace", path="/name", value=new_name
        )
        response_json = parse_json(response=response)
        return Group(response_json) if response_json else None
    
    def delete_group(self, uuid):
        """
        Delete a group by UUID
        @param uuid: UUID of the group
        @return: Boolean indicating success or failure
        """
        url = f"{self.API_ENDPOINT}/eperson/groups/{uuid}"
        response = self.api_delete(url, params=None)
        if response.status_code == 204:
            return True
        else:
            logging.error("Failed to delete group %s: %s", uuid, response.text)
            return False
    
    def get_group_by_uuid(self, uuid, embeds=None):
        """
        Fetch a single group by UUID
        @param uuid: UUID of the group
        @param embeds: Optional list of resources to embed in response JSON
        @return: Group object or None if not found
        """
        url = f"{self.API_ENDPOINT}/eperson/groups/{uuid}"
        params = parse_params(embeds=embeds)
        response = self.api_get(url, params=params)
        response_json = parse_json(response=response)
        return Group(response_json) if response_json else None
    
    def start_workflow(self, workspace_item):
        url = f"{self.API_ENDPOINT}/workflow/workflowitems"
        res = parse_json(self.api_post_uri(url, params=None, uri_list=workspace_item))
        logging.debug(res)
        # TODO: WIP

    def update_token(self, r):
        """
        Refresh / update the XSRF (aka. CSRF) token if DSPACE-XSRF-TOKEN found in response headers
        This is used by all the base methods like api_put,
        See: https://github.com/DSpace/RestContract/blob/main/csrf-tokens.md
        :param r:
        :return:
        """
        if not self.session:
            logging.debug("Session state not found, setting...")
            self.session = requests.Session()
        if "DSPACE-XSRF-TOKEN" in r.headers:
            t = r.headers["DSPACE-XSRF-TOKEN"]
            logging.debug("Updating XSRF token to %s", t)
            # Update headers and cookies
            self.session.headers.update({"X-XSRF-Token": t})
            self.session.cookies.update({"X-XSRF-Token": t})

    def get_short_lived_token(self):
        """
        Get a short-lived (2 min) token in order to request restricted bitstream downloads
        @return: short lived Authorization token
        """
        if not self.session:
            logging.debug("Session state not found, setting...")
            self.session = requests.Session()

        url = f"{self.API_ENDPOINT}/authn/shortlivedtokens"
        r = self.api_post(url, json=None, params=None)
        r_json = parse_json(r)
        if r_json is not None and "token" in r_json:
            return r_json["token"]

        logging.error("Could not retrieve short-lived token")
        return None

    def solr_query(self, query, filters=None, fields=None, start=0, rows=999999999):
        """
        Perform raw Solr query
        @param query: query string
        @param filters: list of filter queries
        @param fields: list of fields to return in results
        @param start: start doc
        @param rows: max docs to return
        @return: solr search results
        """
        if fields is None:
            fields = []
        if filters is None:
            filters = []
        return self.solr.search(
            query, fq=filters, start=start, rows=rows, **{"fl": ",".join(fields)}
        )

    def resolve_identifier_to_dso(self, identifier=None):
        """
        Resolve a DSO identifier (uuid, handle, DOI, etc.) to a DSO URI
        Useful for resolving handles to objects, etc.
        @param identifier: a persistent identifier for an object like handle, doi, uuid
        @return: resolved DSpaceObject or error
        """
        if identifier is not None:
            url = f'{self.API_ENDPOINT}/pid/find'
            r = self.api_get(url, params={'id': identifier})
            if r.status_code == 200:
                r_json = parse_json(r)
                if r_json is not None and 'uuid' in r_json:
                    return DSpaceObject(api_resource=r_json)
            elif r.status_code == 404:
                logging.error(f"Not found: {identifier}")
            else:
                logging.error(f"Error resolving identifier {identifier} to DSO: {r.status_code}")
