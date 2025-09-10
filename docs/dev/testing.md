# Testing

To test this project we used the JavaScript testing framework called [vitest](https://vitest.dev/). Since [Vite](https://vitejs.dev/), which is a JavaScript bundler, was used to compile the project, it was decided to use the testing framework that comes integrated with Vite. This framework was also chosen because it is very similar to [Jest](https://jestjs.io/), which is the most popular testing framework for JavaScript. They have practically the same syntax and work in the same way.

## Unit tests structure

The tests are located in the `tests` folder and the idea is that the structure is the same as the `src` folder. For example, if you want to test the component `src/components/MyComponent.jsx`, the test should be created in `tests/components/MyComponent.test.jsx`.

## Setup and configuration files

There are two setup files to prepare the test environment. The first is `tests/test-utils.jsx` and the second is `tests/setup.js`.

### setup.js

In the `tests/setup.js` file the [MSW](https://mswjs.io/) Mock server is prepared. This Mock server intercepts HTTP requests made from the application and returns a fake response. This is very useful to test the application without having to make real requests to the API.

Basically in this file are defined all the endpoints that we are going to use in the tests. In addition, it is configured to give an error if a request is made to an endpoint that is not defined. This is useful to avoid making real requests to the API by mistake.

This file also mocks some DOM elements that are used in the tests. For example, the `window.location` is mocked so that it can be used within the testing environment.

### test-utils.jsx

In this file two important things are set up:

- On the one hand, a wrapper is defined to load the different providers that are needed to assemble the React components, and not to have to repeat the code in each test.

- On the other hand, we extend the `expect()` methods of vitest to have available all the methods that jest has. This is useful to be able to use methods like `.toBeInTheDocument()` that do not come by default in vitest.

The way to use this file in the test files is to import the methods from this file instead of vitest directly like this:

```javascript
import { render } from "../test-utils";
```

Using this render function will also render the providers defined in the wrapper.

## Running the tests

To run the tests you must execute the following command:

```bash
npm run test
```

This will start the MSW Mock server and run the tests. By default it is run in watch mode so that the tests are re-run every time a file is modified.

If you only want to run it once:

```bash
npm run test run
```

## Environment variables

The variables to be used in the tests must be defined in the `.env.test` file. This file is in the root of the project.
