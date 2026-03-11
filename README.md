Tiketo Pass Manager
===================

Keboola component for managing digital passes via the Tiketo CMS GraphQL API.

**Table of Contents:**

[TOC]

Functionality Notes
===================

This component provides full integration with the Tiketo CMS API, supporting extraction
of all entity types and write operations including pass management, member management,
venue management, organization management, and push notification campaigns.

Prerequisites
=============

- A Tiketo CMS account with an API token
- API token can be obtained from the Tiketo CMS workspace settings

Features
========

| **Feature**             | **Description**                                          |
|-------------------------|----------------------------------------------------------|
| Generic UI Form         | Dynamic UI form for easy configuration.                  |
| Incremental Loading     | All extract actions support incremental loading.         |
| Batch Operations        | Efficient batch processing via GraphQL aliases.          |
| Full CRUD               | Create, read, update, delete for all entity types.       |
| Push Notifications      | Send campaigns to specific passes or entire templates.   |

Supported Actions
=================

**Extract (read from Tiketo):**
- `list_templates` - List all pass templates
- `list_passes` - List all passes with parameters and share URLs
- `list_members` - List all members
- `list_venues` - List all venues
- `list_organizations` - List all organizations
- `list_campaigns` - List all message campaigns

**Write - Passes:**
- `upsert_passes` - Create or update passes (input: template_id, id, parameters...)
- `delete_passes` - Delete passes by ID

**Write - Members:**
- `upsert_members` - Create or update members (input: email, phone, name...)
- `delete_members` - Delete members by ID

**Write - Venues:**
- `upsert_venues` - Create or update venues
- `delete_venues` - Delete venues by ID
- `add_venue_members` / `remove_venue_members` - Manage venue membership

**Write - Organizations:**
- `upsert_organizations` / `delete_organizations` - CRUD operations
- `move_organizations` - Move org to new parent
- `add_organization_members` / `remove_organization_members` - Manage org membership
- `update_organization_member_roles` - Update member roles
- `attach_entities_to_organizations` / `detach_entities_from_organizations` - Entity management

**Write - Campaigns:**
- `create_passes_campaign` - Send notification to specific passes
- `create_template_campaign` - Send notification to all passes of templates
- `archive_campaigns` - Archive campaigns

Configuration
=============

API Token
---------
Your Tiketo CMS API token. Obtain from workspace settings.

Action
------
Select which operation to perform from the list of supported actions above.

Batch Size
----------
Number of items to process per API request (1-100, default 50).

Output
======

Extract actions produce output tables with incremental loading enabled:
- `templates.csv` - id, name
- `passes.csv` - id, template_id, member_id, parameters, expiration_date, voided, share_url, created_at, updated_at
- `members.csv` - id, email, phone, externalId, firstName, lastName, metadata, ...
- `venues.csv` - id, name, description, type, address, metadata, ...
- `organizations.csv` - id, name, description, parentId, path, depth, metadata, ...
- `campaigns.csv` - id, type, status, messageHeader, messageBody, totalCount, sentCount, failedCount, ...

Write actions produce result tables confirming the operation.

Development
-----------

Clone this repository, initialize the workspace, and run the component:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone https://github.com/kbcMichal/app-tiketo-pass-manager.git
cd app-tiketo-pass-manager
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and perform lint checks:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For details about deployment and integration with Keboola, refer to the
[deployment section of the developer
documentation](https://developers.keboola.com/extend/component/deployment/).
