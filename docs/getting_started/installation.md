# Installation and Setup

This application has been developed in JavaScript using the React framework and compiled using [Vite](https://vitejs.dev/).

To install the dependencies and run the app in development mode, you first need to install Node on your computer. See [Node Downloads](https://nodejs.org/en/download) to learn how to install Node using the package manager of your operating system.

```{note}
The version of Node used to develop this app is **18.16.0**. If you have problems running the app, try to install this version.
```

## Dependencies

| **Dependency**                                                   | **Version** | **Description**                                                                                               |
| ---------------------------------------------------------------- | ----------- | ------------------------------------------------------------------------------------------------------------- |
| [React](https://es.react.dev/)                                   | 17.0.2      | JavaScript library for building user interfaces                                                               |
| [react-router-dom](https://github.com/remix-run/react-router)    | 6.11.2      | Library to manage the routes of the app                                                                       |
| [EUI](https://elastic.github.io/eui/#/)                          | 81.2.0      | Components Library for Elastic UI                                                                             |
| [axios](https://axios-http.com/docs/intro)                       | 1.4.0       | Promise based HTTP client for the browser. We use this to perform the request to the Data API and Auth Server |
| [react-query](https://tanstack.com/query/v3/docs/react/overview) | 3.39.3      | Library to manage the state of the app and orchestrate the HTTP requests                                      |
| [use-debounce](https://github.com/xnimorz/use-debounce)          | 9.0.4       | Library to debounce the search input                                                                          |
| [react-hook-form](https://react-hook-form.com/)                  | 7.44.3      | Library to manage the form state and validation                                                               |
| [yup](https://github.com/jquense/yup)                            | 1.2.0       | Library to validate the form data                                                                             |

## Run in development mode

To run the app in development mode, you first need to install the dependencies. To do so, go to `uglyData/frontend` where package.json is located and run:

```bash
npm install
```

Once the dependencies are installed, you can run the app in development mode with:

```bash
npm run dev
```

This will start the app in development mode on [http://localhost:3000](http://localhost:3000).
