# DataViewer documentation

- **version**: 2.1.11 (See [Changelog])
- **date**: 4th June 2024

**DataViewer** is a [CRUD](https://es.wikipedia.org/wiki/CRUD) web app made with [React](https://es.react.dev/) and using [Elastic UI](https://elastic.github.io/eui/) Components Library. It allows you to authentify as a user and perform actions on the database data like add, update or delete.

It also provides tools like the Market Data Searcher to see the available data on the database for each instrument.

The **main goal** of this documentation is to help you understand the code of the app, how to contribute to the development and how to deploy it to production.

::::{grid} 2
:gutter: 5

:::{grid-item-card} Getting Started
:link: getting_started/index.html
:text-align: center

{octicon}`book;5em;sd-text-info`
^^^
Check the getting started guides to learn how to deploy the app to production and how to run it in development mode.

:::
:::{grid-item-card} Project Structure
:text-align: center
:link: project_structure/index.html

{octicon}`file-directory;5em;sd-text-info`
^^^
Learn how the project is structured and how the different parts of the app work together.

:::
:::{grid-item-card} API Reference
:text-align: center
:link: modules/modules.html
{octicon}`code;5em;sd-text-info`
^^^
Learn how to use the different components of the app.

:::
::::{grid-item-card} Development Guidelines
:text-align: center
:link: dev/index.html
{octicon}`terminal;5em;sd-text-info`
^^^
If you want to contribute to the development take a look to the development guidelines first.

:::::::

```{toctree}
---

hidden:
---
getting_started/index.md
project_structure/index.md
modules/modules.rst
dev/index.md
changelog.md
```
