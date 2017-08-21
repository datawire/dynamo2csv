# scout.py

[![Build Status](https://travis-ci.org/datawire/scout.py.svg?branch=master)](https://travis-ci.org/datawire/scout.py)

Scout.py is our usage reporting API client for Python clients at Datawire. Scout.py collects some basic information about users so it can send important client notices such as new version availability and security bulletins. We also use the information to anonymously aggregate basic usage analytics.

## Why?

- We want to know how you are using our software, so we can make it better for you. Knowing what versions are being used, in aggregate, is very helpful for development and testing.
- We ship new releases frequently, with new features and bug fixes. We want you to know when we ship a new release.

## What is collected?

The following information is collected and sent during version checks:

- Application Name
- Application Version
- Install Identifier (locally generated for each app and stored in `${HOME}/.config/${APP}/id`)
- Platform Information (Operating System, Python version)

The reporting code can be found in [scout.py](scout/scout.py).

## Can it be disabled?

Yes! Set an environment variable `SCOUT_DISABLE=1`.

# License

Licensed under Apache 2.0. Please read [LICENSE](LICENSE) for details.
