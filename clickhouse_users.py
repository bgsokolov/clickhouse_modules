#!/usr/bin/python3

# Copyright: (c) 2024, Boris Sokolov <msc.bsokolov@gmail.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
from __future__ import (absolute_import, division, print_function)

__metaclass__ = type

DOCUMENTATION = r'''
---
module: clickhouse_query

short_description: The module to create and update clickhouse users.

version_added: "1.0.0"

description: The module run SQL queries on remote hosts using clickhouse-driver to create/update clickhouse users and 
apply quotas, profiles and roles.

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
'''

EXAMPLES = r'''
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
'''

RETURN = r'''
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
'''

import re
from contextlib import suppress

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.common.text.converters import to_native

CHClient = False
with suppress(ImportError):
    from clickhouse_driver import Client as CHClient, errors as ch_errors


def ch_user_exists(ch_connect, user):
    user_exists = ch_connect.execute(f"SELECT count() FROM system.users WHERE name = '{user}'")[0][0] > 0
    return user_exists


def ch_user_roles(ch_connect, user, roles):
    user_roles = []
    user_has_roles = True
    user_roles_query = f"SELECT granted_role_name FROM system.role_grants WHERE user_name = '{user}';"
    [user_roles.append(role[0]) for role in ch_connect.execute(user_roles_query)]
    for role in roles:
        if role not in user_roles:
            user_has_roles = False
    return user_roles, user_has_roles


def ch_user_profiles(ch_connect, user, profile):
    user_profiles = []
    user_profile_query = f"SELECT inherit_profile FROM system.settings_profile_elements WHERE user_name = '{user}';"
    [user_profiles.append(profile[0]) for profile in ch_connect.execute(user_profile_query)]
    if profile in user_profiles:
        user_has_profile = True
    else:
        user_has_profile = False
    return user_profiles, user_has_profile


def ch_user_quotas(ch_connect, user, quota):
    user_quotas = []
    user_has_quota = False
    quota_applied_users = []
    user_quotas_query = f"SELECT name FROM system.quotas WHERE has(apply_to_list, '{user}');"
    quota_has_users_query = f"SELECT apply_to_list FROM system.quotas WHERE name = '{quota}';"
    # Get quotas applied to user
    [user_quotas.append(quota[0]) for quota in ch_connect.execute(user_quotas_query)]
    raw_quota_users = ch_connect.execute(quota_has_users_query)
    if raw_quota_users:
        quota_applied_users = raw_quota_users[0][0]
    if quota in user_quotas:
        user_has_quota = True
    quota_applied_users.append(user)
    return user_quotas, user_has_quota, quota_applied_users


def create_update_user(ch_connect, user, password, roles, quota, profile):
    # Get user status
    user_exists = ch_user_exists(ch_connect, user)
    # Returned values
    query_list = []
    user_status = {"user_exists": user_exists}
    return_list = {
        "changed": False,
        "run_queries": query_list,
        "user_status": user_status,
    }
    # Create user
    if not user_exists:
        user_query = f"CREATE USER {user} IDENTIFIED WITH sha256_password BY '{password}'"
        query_list.append(user_query)
    # Add quota to user
    if quota != '':
        user_quotas, user_has_quota, quota_apply_users = ch_user_quotas(ch_connect, user, quota)
        user_status["user_quotas"] = user_quotas
        user_status["user_has_quota"] = user_has_quota
        # user_status["quota_apply_users"] = quota_apply_users
        if not user_has_quota:
            quota_query = f"ALTER QUOTA {quota} to {', '.join(quota_apply_users)}"
            query_list.append(quota_query)
    # Set user profile
    if profile != '':
        user_profiles, user_has_profile = ch_user_profiles(ch_connect, user, profile)
        user_status["user_profiles"] = user_profiles
        user_status["user_has_profile"] = user_has_profile
        if not user_has_profile:
            profile_query = f'ALTER USER {user} SETTINGS PROFILE {profile}'
            query_list.append(profile_query)
    #  Grant roles to user
    if roles:
        user_roles, user_has_roles = ch_user_roles(ch_connect, user, roles)
        user_status["user_roles"] = user_roles
        user_status["user_has_roles"] = user_has_roles
        if not user_has_roles:
            roles_query = f"GRANT {', '.join(roles)} to {user}"
            query_list.append(roles_query)
    # Run queries
    if query_list:
        [ch_connect.execute(query) for query in query_list]
        return_list["changed"] = True
    return return_list


def delete_user(ch_connect, user):
    user_exists = ch_user_exists(ch_connect, user)
    if not user_exists:
        return {"changed": False, "user_exists": user_exists}
    query = f"DROP USER {user}"
    ch_connect.execute(query)
    return {"changed": True, "query": query, "user_exists": user_exists}


def main():
    # Module's arguments settings
    module_args = {
        "address": {"type": "str", "required": False, "default": "127.0.0.1"},
        "login_user": {"type": "str", "required": False, "default": "default"},
        "login_password": {"type": "str", "required": False, "default": "", "no_log": True},
        "user_name": {"type": "str", "required": True},
        "user_password": {"type": "str", "required": False, "default": "", "no_log": True},
        "user_quota": {"type": "str", "required": False, "default": ''},
        "user_profile": {"type": "str", "required": False, "default": ''},
        "user_roles": {"type": "list", "required": False, "default": []},
        "user_state": {"type": "str", "required": False, "default": "present"}
    }

    # Default run module result
    result = {
        "changed": False
    }

    # Set of states we could make on the user
    states = {'present', 'absent'}

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    # Check clickhouse-driver module is installed
    if CHClient is False:
        return module.fail_json("The clickhouse-driver module is required to install to host")

    # Ansible check mode settings
    if module.check_mode:
        module.exit_json(**result)

    # Module parameters
    address = module.params['address']
    login_user = module.params['login_user']
    login_password = module.params['login_password']
    user = module.params['user_name']
    password = module.params['user_password']
    roles = module.params['user_roles']
    quota = module.params['user_quota']
    profile = module.params['user_profile']
    state = module.params['user_state']

    # Check connection to Clickhouse server
    ch_connect = CHClient(host=address, user=login_user, password=login_password)

    # Try to run CRUD actions for the user
    if state == "present":
        try:
            result = create_update_user(ch_connect, user, password, roles, quota, profile)
        except ch_errors.ServerException as err:
            db_error = re.findall(r'DB::Exception.+\.', err.message)
            return module.fail_json(to_native(db_error))
        except Exception as err:
            return module.fail_json(to_native(err))
    elif state == "absent":
        result = delete_user(ch_connect, user)
    else:
        return module.fail_json(f"Only {states} states are supported by this module")

    module.exit_json(**result)


if __name__ == '__main__':
    main()
