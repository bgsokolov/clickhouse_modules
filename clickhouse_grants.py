#!/usr/bin/python3

# Copyright: (c) 2024, Boris Sokolov <msc.bsokolov@gmail.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
from __future__ import (absolute_import, division, print_function)

__metaclass__ = type

DOCUMENTATION = r'''
---
module: clickhouse_grants

short_description: The module to create and update Clickhouse role grants.

version_added: "1.0.0"

description: The module run SQL queries on remote hosts using clickhouse-driver to add/revoke clickhouse roles grants.

options:
    address:
        description:
          - Listen clickhouse server IP address.
        required: false
        type: str
        default: 127.0.0.1
    secure_connect:
        description:
          - Secure connection settings.
        required: false
        type: bool
        default: false
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
    grantee_name: 
        description:
          - Clickhouse user or role name.
        required: true
        type: str
    grants:
        description:
          - Clickhouse grant type.
        type: list
        elements: str
        required: false
    databases:
        description:
          - List of Clickhouse databases to set grants.
        required: false
        type: list
        elements: str
        default: ['default']
    tables:
        description:
          - Clickhouse tables names.
        required: false
        type: list
        elements: str
        default: [*]
    grant_roles:
        description:
          - List of roles to assign to user.
        type: list
        elements: str
        required: false
    grant_roles_init:
        description:
          - Create roles in case if they are not exist.
        required: false
        type: bool
        default: false
    replace_grants:
        description:
          - Replace existed grants.
        required: false
        type: bool
        default: false
    revoke_grants:
        description:
          - Revoke existed grants.
        required: false
        type: bool
        default: false
    on_cluster:
        description:
          - Run distributed query on cluster.
        type: bool
        required: false
        default: false
    cluster_name:
        description:
          - Clickhouse cluster name to run distributed query.
        type: string
        required: false
        default: 'default' 
author:
    - Boris Sokolov (@bgsokolov)
'''

EXAMPLES = r'''
- name: Grant select and insert for 'reader' to 'dictionaries' database tables. 
  clickhouse_grants:
    login_user: 'default'
    login_password: default's password
    grantee_name: 'reader'
    grants:
        - select
        - insert
    databases: ['dictionaries']
    tables:
        - 'statistics'
        - 'clients'
        
- name: Create 'reader' role and assign it to 'developer' user/role. 
  clickhouse_grants:
    login_user: 'default'
    login_password: default's password
    grantee_name: 'developer'
    grant_roles: ['reader']
    grant_roles_init: true
        
- name: Revoke 'delete' grant from 'reader_role'
  clickhouse_grants:
    login_user: 'default'
    login_password: default's password
    grantee_name: 'reader_role'
    grants:
        - delete
    revoke_grants: true
'''

RETURN = r'''
run_queries:
  description: List of executed queries
  returned: always
  type: list
  sample:
    - GRANT reader_role TO developer
    - GRANT on CLUSTER '{cluster}' select ON statistics.* TO reader_role
    - GRANT on CLUSTER '{cluster}' insert, select ON statistics.* TO reader_role WITH REPLACE OPTION
    - REVOKE on CLUSTER 'default' delete FROM reader_role
'''

import re
from contextlib import suppress

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.common.text.converters import to_native

CHClient = False
with suppress(ImportError):
    from clickhouse_driver import Client as CHClient, errors as ch_errors


def ch_user_exists(ch_connect, grantee):
    user_exists = ch_connect.execute(f"SELECT count() FROM system.users WHERE name = '{grantee}'")[0][0] > 0
    return user_exists


def get_user_roles(ch_connect, grantee, roles_list):
    # Get user status
    if not ch_user_exists(ch_connect, grantee):
        return module.fail_json(f"'{grantee}' user does not exist")
    user_roles = []
    user_has_roles = True
    user_roles_query = f"SELECT granted_role_name FROM system.role_grants WHERE user_name = '{grantee}';"
    [user_roles.append(role[0]) for role in ch_connect.execute(user_roles_query)]
    for role in roles_list:
        if role not in user_roles:
            user_has_roles = False
    return user_roles, user_has_roles


def grant_roles_func(ch_connect, grantee, roles_list, roles_init, replace_grants, revoke_grants):
    # Returned values
    query_list = []
    user_status = {}
    return_list = {
        "changed": False,
        "run_queries": query_list,
        "grant_roles": roles_list,
        "revoke_grants": revoke_grants,
        "replace_grants": replace_grants
    }

    # Get existed user roles
    user_roles, user_has_roles = get_user_roles(ch_connect, grantee, roles_list)
    user_status["user_roles"] = user_roles
    user_status["user_has_roles"] = user_has_roles

    # Revoke roles from user
    if revoke_grants:
        for role in roles_list:
            if role in user_roles:
                role_query = f"REVOKE {role} from '{grantee}'"
                query_list.append(role_query)

    else:
        # Create roles if needed
        if roles_init and not user_has_roles:
            for role in roles_list:
                create_role_query = f"CREATE ROLE IF NOT EXISTS {role}"
                query_list.append(create_role_query)

        # Grant roles to user with replace option
        if replace_grants:
            grant_roles_query = f"GRANT {', '.join(roles_list)} to '{grantee}' WITH REPLACE OPTION"
            query_list.append(grant_roles_query)

        # Grant roles to user
        elif not user_has_roles:
            grant_roles_query = f"GRANT {', '.join(roles_list)} to '{grantee}'"
            query_list.append(grant_roles_query)

        return_list.update({"user_status": user_status})

    # Run queries if query_list not empty
    if query_list:
        [ch_connect.execute(query) for query in query_list]
        return_list["changed"] = True

    return return_list


def grants_func(ch_connect, grantee, grants_list, databases, tables, replace_grants, revoke_grants):
    # Returned values
    query_list = []
    return_list = {
        "changed": False,
        "run_queries": query_list,
    }

    system_level_grants = [
        "CREATE FUNCTION",
        "DROP FUNCTION",
        "RELOAD DICTIONARY",
        "KILL QUERY",
        "MYSQL",
        "CLUSTER"
    ]

    database_level_grants = [
        "CREATE DATABASE",
        "DROP DATABASE"
    ]

    table_level_grants = [
        "ALL",
        "SELECT",
        "SHOW",
        "dictGet",
        "INSERT",
        "UPDATE",
        "DELETE",
        "ALTER",
        "ALTER TABLE",
        "ALTER COLUMN",
        "ALTER CONSTRAINT",
        "ALTER INDEX",
        "ALTER VIEW",
        "ALTER TTL",
        "CREATE",
        "CREATE TABLE",
        "CREATE VIEW",
        "CREATE DICTIONARY",
        "DROP",
        "DROP TABLE",
        "DROP VIEW",
        "DROP DICTIONARY",
        "TRUNCATE",
        "OPTIMIZE",
    ]

    # Grants divided by types for future needs
    applicable_grants = system_level_grants + database_level_grants + table_level_grants

    for grant in grants_list:
        # Check grants set correctly
        if grant.upper() not in applicable_grants and grant != 'dictGet':
            return_list["error"] = f"{grant.upper()} not in applicable grants: {', '.join(applicable_grants)}."
            return_list["failed"] = True
            return return_list

    for db_idx, database in enumerate(databases):
        for tb_idx, table in enumerate(tables):
            if not revoke_grants:
                sub_query = " WITH REPLACE OPTION" if (replace_grants and db_idx == 0 and tb_idx == 0) else ""
                grant_query = f"GRANT {', '.join(grants_list)} on {database}.{table} to '{grantee}'" + sub_query
                query_list.append(grant_query)
            else:
                grant_query = f"REVOKE {', '.join(grants_list)} on {database}.{table} from '{grantee}'"
                query_list.append(grant_query)

    # Run queries if query_list not empty
    if query_list:
        [ch_connect.execute(query) for query in query_list]
        return_list["changed"] = True
        return_list["msg"] = "GRANTS EXECUTED"

    return return_list


def main():
    # Module's arguments settings
    module_args = {
        "address": {"type": "str", "required": False, "default": "127.0.0.1"},
        "secure_connect": {"type": "bool", "required": False, "default": False},
        "login_user": {"type": "str", "required": False, "default": "default"},
        "login_password": {"type": "str", "required": False, "default": "", "no_log": True},
        "grantee_name": {"type": "str", "required": True},
        "grants": {"type": "list", "required": False, "default": []},
        "databases": {"type": "list", "required": False, "default": ["default"]},
        "tables": {"type": "list", "required": False, "default": "*"},
        "grant_roles": {"type": "list", "required": False, "default": []},
        "init_roles": {"type": "bool", "required": False, "default": False},
        "revoke_grants": {"type": "bool", "required": False, "default": False},
        "replace_grants": {"type": "bool", "required": False, "default": False},
        "on_cluster": {"type": "bool", "required": False, "default": False},
        "cluster_name": {"type": "str", "required": False, "default": "default"}
    }

    # Default run module result
    result = {
        "changed": False
    }

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
    secure_connect = module.params['secure_connect']
    login_user = module.params['login_user']
    login_password = module.params['login_password']
    grantee = module.params['grantee_name']
    grants_list = module.params['grants']
    databases = module.params['databases']
    tables = module.params['tables']
    roles_list = module.params['grant_roles']
    roles_init = module.params['init_roles']
    revoke_grants = module.params['revoke_grants']
    replace_grants = module.params['replace_grants']
    on_cluster = module.params['on_cluster']
    cluster = module.params['cluster_name']

    # Check connection to Clickhouse server
    ch_connect = CHClient(host=address, user=login_user, password=login_password, secure=secure_connect, verify=False)

    # Make checks and run grant functions
    if roles_list and grants_list:
        return module.fail_json(f"Only one of parameters 'grant_roles' OR 'grants' must be defined.")

    elif roles_list:
        try:
            result = grant_roles_func(ch_connect, grantee, roles_list, roles_init, replace_grants, revoke_grants)
        except ch_errors.ServerException as err:
            db_error = re.findall(r'DB::Exception.+\.', err.message)
            return module.fail_json(to_native(db_error))
        except Exception as err:
            return module.fail_json(to_native(err))

    elif grants_list:
        try:
            result = grants_func(ch_connect, grantee, grants_list, databases, tables, replace_grants, revoke_grants)
        except ch_errors.ServerException as err:
            db_error = re.findall(r'DB::Exception.+\.', err.message)
            return module.fail_json(to_native(db_error))
        except Exception as err:
            return module.fail_json(to_native(err))

    else:
        return module.fail_json(f"No any grants or roles are defined")

    module.exit_json(**result)


if __name__ == '__main__':
    main()
