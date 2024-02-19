## Ansible Modules and Plugins

Project to keep custom ansible modules. List of modules and usage examples you'll find below. 

clickhouse_user.py
------------

### Requirements

[clickhouse-driver](https://clickhouse-driver.readthedocs.io/en/latest/installation.html) (Python library)

    pip3 install clickhouse-driver

### DOCUMENTATION

```
---
module: clickhouse_users

short_description: The module to create and update clickhouse users.

version_added: "1.0.0"

description: The module run SQL queries on remote hosts using clickhouse-driver to create/update clickhouse users and apply quotas, profiles and roles.

options:
    address:
        description:
          - Listen clickhouse server IP address.
        required: false
        type: str
        default: 127.0.0.1
    login_user: 
        description:
          - Clickhouse user for login to DB.
        required: false
        type: str
        default: default
    login_password:
        description:
          - Clickhouse password for login to DB.
        required: false
        type: str
    user_name: 
        description:
          - Clickhouse user name.
        required: true
        type: str
    user_password:
        description:
          - Clickhouse new user password.
        required: false
        type: str
    user_roles:
        description:
          - Clickhouse user roles.
        required: false
        type: list
        elements: str
    user_profile:
        description:
          - Clickhouse user profile.
        required: false
        type: str
    user_quota:
        description:
          - Clickhouse user quota.
        required: false
        type: str
    user_state:
        description:
          - State of Clickhouse user.
        required: false
        type: str
        choices: [absent, present]
        default: present
author:
    - Boris Sokolov (@bgsokolov)
```
### EXAMPLES
```
- name: Create user if not exists and/or make update of user's quota, profile or roles. 
  clickhouse_users:
    login_user: 'default'
    login_password: default's password
    user_name: 'username'
    user_password: user's password
    user_roles:
        - 'role_1'
        - 'role_2'
    user_profile: 'custom_profile'
    user_quota: 'custom_quota'
        
- name: Connect to DBMS clickhouse and delete user
  clickhouse_users:
    login_user: default
    login_password: default's password
    user_name: user_to_delete
    user_state: absent
```

### RETURN
```
user_status:
  description: List of user statuses
  type: dict
  returned: always
  sample:
      user_exists: false
      user_has_profile: false
      user_has_quota: false
      user_has_roles: false
      user_profiles: []
      user_quotas: []
      user_roles: []
run_queries:
  description: List of executed queries
  returned: always
  type: list
  sample:
    - CREATE USER test_user IDENTIFIED WITH sha256_password BY '********'
    - ALTER QUOTA test_quota to test_user, test_user_1, test_user_2
    - ALTER USER test_user SETTINGS PROFILE test_profile
    - GRANT test_role_1, test_role_2 to test_user
```