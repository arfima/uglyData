# Getting Started

Here you will find the guides to get started with the DataViewer app.

```{important}
The **DataViewer** will not work properly if you don't have the [Data API] running and connected to the database.

The API setup is located in `uglyData/frontend/.env.development` for development mode and `uglyData/frontend/.env.production` for production mode.

```

::::{grid} 1
:gutter: 5

:::{grid-item-card}
:text-align: left

**Adding instruments and other entities**
^^^
The page allows you to add a new element if it is not already there (except for product_types.)

So you can start adding an instrument and in the process add a product and family or whatever is needed. 

+++

```{button-link} deployment.html
:color: primary
:shadow:
See more
```

:::

:::{grid-item-card}
:text-align: left

<!-- :link: installation.html -->

**Run in development mode**
^^^
Once you have Node installed, you can install the dependencies and run the app in development mode.

Go to `uglyData/frontend` where package.json is located and run:

```bash
npm install
```

To run the app in development mode, run:

```bash
npm run dev
```

This will start the app in development mode. Open [http://localhost:3000](http://localhost:3000) to view it in the browser.

+++

```{button-link} installation.html
:color: primary
:shadow:
See more
```

:::

:::{grid-item-card}
:text-align: left

**Deployment**
^^^
To deploy the app to production, we use Docker alongside Docker Compose.

To deploy, go to `uglyData/frontend` where `docker-compose.yml` is located and run:

```bash
docker-compose up -d
```

This will build the Docker image and start the container.

To deploy the DataViewer with the backend see the uglyData [deployment guide].

+++

```{button-link} deployment.html
:color: primary
:shadow:
See more
```

:::

::::

```{toctree}
---
caption: Getting Started
hidden:
---
addInstruments
installation
deployment
```
