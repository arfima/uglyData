# Development Guidelines

## Style Guide

The code style is enforced using [Prettier](https://prettier.io/). The configuration files are located in the root of the project. There is a pre-commit hook that runs Prettier before committing the code. If the code is not formatted correctly, the commit will fail.

Also, there is a VSCode extension that runs Prettier every time a file is saved. This extension is called [Prettier - Code formatter](https://marketplace.visualstudio.com/items?itemName=esbenp.prettier-vscode).

## Comments

The comments are written in English and using the [JSDoc](https://jsdoc.app/) syntax. This is useful to generate the documentation of the project automatically in case it is needed in the future.

All components, method classes and public functions should be documented. Describing what the component/method/function does and what props or parameters it receives, as well as the type of each prop/parameter and what it returns.

## Documentation

The documentation is generated automatically using Sphinx because is the tool that is used to generate the documentation of the Python projects. The documentation files (.md) are located in the `docs` folder.

In the documentation there is a section that overviews the project structure. This section should be updated every time a new folder is added to the project or when necessary.

## Changelog

Changelog is updated manually in the `docs/changelog.md` file. The format of the changelog is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/). The changelog should be updated every time a new deploy is made.
