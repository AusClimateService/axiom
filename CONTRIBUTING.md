# Contribution Guide

Thank you for showing an interest in contributing to Axiom. This guide will serve as reference for those wanting to contribute to the ongoing development of the software.

## Where to start?

All contributions to the project are welcome.

If you are new to Axiom or open-source development, please go through the Github Issues tab to peruse the current state of development. Issues marked with "good first issue" are a good place to start.

## Reporting bugs

If the software produces errors under certain circumstances, we want to know about them, as it increases the utility and stability of the system.

Please see https://stackoverflow.com/help/minimal-reproducible-example for an overview of writing good bug reports.

Please raise all bug reports as issues on the main repository where they will be triaged by the core development team.

**Note:** Please search the issue tracker to see if anyone else has raised a similar issue and contribute to that discussion instead of raising your own.

In general, a bug report should contain 3 things:

1. A Python snippet or CLI command that triggers the bug.
2. A stack trace or log information showing the error (please redact anything sensitive you don't want made public).
3. The current version of Axiom you are using.

## Feature requests

Requests for new features, like bug reports, are to be made by raising an issue. As with bugs, please search for existing similar issues and contribute to that discussion before raising your own.

At minimum, a feature request should answer the following questions:

- What is the desired functionality?
- What is the use-case for this feature?
- How will the user interact with the feature?

It would also be useful to supply a suggested method call that could be used to trigger the new feature.

For Example:

```python
import axiom as ax
ax.my_super_new_feature(True)
```

Bear in mind, not all features should or will be implemented, particularly if they stray too far from the central objectives of the software. In these cases we recommend using Axiom as an included library in your own package for your own specific purposes.