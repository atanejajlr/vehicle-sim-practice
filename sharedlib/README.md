# Vsim Shared Resources

# Sharedlib

Shared operators and utilities for Apache Airflow

## Concept

We need a way to share code among multiple Airflow DAGs. Idea is to use git submodule.
Using submodule, we can keep common code in single place (this repository) and reference it in Vsim project.

## User Guide

1. Add to new project. From project's top-most directory, run this command:
```bash
git submodule add git@gitlab.jlr-ddc.com:indigital/vehicle-data-model/vsim-shared-resources.git sharedlib
```

If you want to use the https then, use the below command:
```bash
git submodule add https://<gitlab_user_id>:<User_Access_Token>@gitlab.jlr-ddc.com/indigital/vehicle-data-model/vsim-shared-resources.git sharedlib
```

Now you can use it like any other Python module:
```
from sharedlib.helpers import function
```

2. 1st step creates `.gitmodules` in your project structure. This git managed file contains submodule path and url info.


3. Check submodule version
```bash
git submodule status
```

4. Commit submodule (1st step adds sharedlib to git tracking as new file)
```bash
git commit -m "add sharedlib"
```
5. Update submodule to latest
```bash
git submodule update --remote
```

6. Remove submodule from project
```bash
git rm -rf sharedlib
rm -rf sharedlib
```

After removed the sharedlib, want to add the same submodule again, then use --force:
```
git submodule add https://<gitlab_user_id>:<User_Access_Token>@gitlab.jlr-ddc.com/indigital/vehicle-data-model/vsim-shared-resources.git sharedlib
```
