# Project Structure

In this section you will learn how the project is structured and how the different parts of the app work together.

## Overview

The project is structured in multiple folders, each one with a different purpose. The main folders are:

- [routers](#routers): contains the routes of the app. There are private routes that can only be accessed if the user is logged in.
- [auth](#auth): contains the components and services that are used to authentify the user.
- [layouts](#layouts): contains the components that are used to build the layout of the app. Like
- [pages](#pages): contains the pages of the app. Each page has its own folder, which contains the components that are used in that page. There are one page for each DataView.
  the header, topbar, sidebar, etc.
- [components](#components): contains the components that are used in more than one page.
- [providers](#providers): contains the providers and custom hooks of the app. These are used throughout the app to provide data or functions to the components.
- [services](#services): contains the services that are used to make requests to the backend or to the auth server.
- [schemas](#schemas): contains the schemas of the data that is used in the app. It contains all the fields to expect from the backend.
- `assets`: contains the assets of the app, like images, fonts, etc.
- `css`: contains the custom css files of the app.
- `utils`: contains the utility functions used throughout all the project.
  (routers)=

## Routers - `src/routers`

The routers are the entry point of the app. They are the ones that define the routes of the app and the components that are rendered in each route. The main component is the `AppRouter` which is the one that is rendered in the `app.jsx` file. It handles all the routes of the app.

In addition to the `AppRouter`, the two types of routes existing in the app are also defined here: private routes and public routes. Private routes are those that can only be accessed if the user is logged in. This is achieved using the `PrivateRoute` component, which is responsible for checking if the user is logged in. If not, it redirects the user to the login page. On the other hand, public routes are those that can be accessed whether the user is logged in or not. This is achieved using the `PublicRoute` component. If they are, it redirects the user to the main page.

The `AppRouter` also provides the `DashboardLayout` component to all pages that need it. Basically all pages except the login page. More details in the [Layouts](#layouts) section.

**How to check if the user is authentified?**

The user is authentified using the `AuthProvider`. The `AuthProvider` is a component that wraps the `AppRouter` and provides the `useAuth` hook to all the components that are inside it. The `useAuth` hook returns the AuthContext which contains the user data and the functions to check if the user is logged. The `PrivateRoute` component uses the `useAuth` hook to check if the user is logged in. If not, it redirects the user to the login page. More details in the next section.
(auth)=

## Auth - `src/auth`

Here are all the components to handling the user authentification process.

The main component is the `AuthProvider` which wraps the entire app. It provides the `useAuth` hook to all the components that are inside it. The `useAuth` hook returns the AuthContext which contains the following properties:

- `user`: the user object. It contains the user data like the name, username and the table access permissions.
- `token`: the token of the user. It is used to make requests to the backend.
- `setToken()`: function to set the token of the user. It is used by the `TokenProvider` (login with Gitlab) or the `Login` component.
- `logout()`: function to logout the user.
- `isLogged()`: boolean that indicates if the user is logged in or not. Basically checks if there is a valid token stored.

The user can log in using two methods:

- **Gitlab**: the user is redirected to the Gitlab login page. After the user logs in, Gitlab redirects the user to the app with a code. The `TokenProvider` component uses this code to ask for a token to the Gitlab server. The token is then stored in the local storage of the browser and the `AuthProvider` is notified to set the token and fetch the user data from the backend.

- **Local login**: the user can also log in using the `Login` page providing an username and password. The `Login` page uses the `AuthService` to make a request to the auth server to log in the user. If the login is successful, the `AuthProvider` is notified to set the token and fetch the user data from the backend.

The token is also stored in the local storage of the browser. This way, when the user refreshes the page, the `AuthProvider` checks if there is a token in the local storage. If there is, it sets the token and fetches the user data from the backend directly.

To access to the AuthContext we use the useAuth hook. This hook is used to get the AuthContext from the `AuthProvider`. It is used in all the components that need to access the user data or the functions to check if the user is logged in.

You can use it like this:

```javascript
import { useAuth } from "auth/useAuth";

const MyComponent = () => {
  const { user, logout } = useAuth();

  return (
    <div>
      <p>{user.name}</p>
      <button onClick={logout}>Logout</button>
    </div>
  );
};
```

(layouts)=

## Layouts - `src/layouts`

The layouts are the components that define the layout of the app. They are the ones that define the header, topbar, sidebar, etc. The main layout is the `DashboardLayout` which is the one that is rendered in the `AppRouter`. It handles the header, topbar, sidebar and the main content of the app. Then it renders one of the pages of the app.

The components that compose the `DashboardLayout` are:

- `Sidebar`: the left sidebar of the app. It contains the links to the different pages of the app. This is the main navigation element. The links are enabled or disable depending on the permissions of the user. The permissions are stored in the `user` object of the `AuthContext`. The `Sidebar` component uses the `useAuth` hook to get the user object and check the permissions.
- `Topbar`: the topbar of the app. It shows the logo, name and version of the app. The more relevent element of the Topbar is the `UserPanel` component. When mounted it fetches the user data from the auth server and renders the user profile: name, username and avatar.
- `PageTitle`: the title of the page. It is rendered above the main content of the page.
  (pages)=

## Pages - `src/pages`

The pages are the components that are rendered in the routes of the app. They are the ones that define the main content of the app.

The pages are:

- `Login`: the login page. It contains the login form and the buttons to login with Gitlab or with username and password. This is the only page that is public. It can be accessed whether the user is logged in or not.
- `MarketData`: It contains a table containing all the available market data in the database. It has a search bar and filters buttons.
- `Log`: The log of the app. All actions performed by users are recorded here.
- `Changelog`: The changelog of the app. It stores all the changes made to the database. The content of this component is based on the file ``src/docs/changelog.md`.
- Table Pages: these pages are the ones that render tables of the database. They are: `Products`, `Exchanges`, `Instruments`, `Drivers`, `Families`, `Subfamilies`, `Events` and `Users`.

All the table pages has a `DataViewGrid` component. This component is responsible for fetching the data from the backend and rendering the table. It also handles the pagination, search and filters of the table. This component is located on `src/components/DataViewGrid`. Is is one of the most complex components of the app.

It handles the following features:

- **Pagination**: the table is paginated. The user can change the page using the buttons on the bottom of the table. Each page is fetched from the backend on demand.
- **Search**: the table has a search bar. The user can search for a specific value in the table. The search is performed on the backend. The search text is highlighted in the table.
- **Filters**: the table has multiple filters buttons. The user can filter the table by one or more values. The filters are performed on the backend.
- **Sorting**: the table can be sorted by one or more columns. The sorting is performed on the backend.
- **Actions**: can handle multiple actions. These are add, edit or delete an item. The add and edit actions are performed using forms. The delete action is performed using a confirmation dialog.

(components)=

## Components - `src/components`

This folder contains the components that are used in the pages. They are the ones that define the main content of the app.

The components are:

- `AddableSelect`: a select component that allows to add new values to the select. It fetches the values from the backend. It is used in the forms with input type "asset".
- `ArfimaLogo`: the logo of the app. It is used in the `Topbar` and `Login`.
- `DataViewGrid`: the grid component that renders the tables of the database. It is used in the table pages.
- `LoadingSpinner`: a loading spinner component. It is used in multiple places of the app when loading lazy imports.
- `RemoveModal`: a confirmation dialog component. It is used in the `DataViewGrid` to confirm the deletion of an item.
- `TagDisplay`: a component that displays a list of tags. It is used in the `DataViewGrid` to display the tags of an item.
- `UserPanel`: a component that displays the user profile. It is used in the `Topbar`.

```{note}
The ``asset`` field type is a custom datatype in this application to refer to a table column that references an identifier from another table. And therefore it has to exist previously in that table. This type of fields are treated in a special way throughout the application and it will be mentioned multiple times in this documentation.
```

### DataViewGrid

This component represents a table of the database and also allows the user to perform actions on the table. It is a wrapper of the EuiDataGrid component of Elastic UI and it adds multiple features to it.

The `DataViewGrid` primary features are adding a new item or edit an item of the table. For this, it use a lateral flyout (which is similar to a modal). The flyout contains the form that allows the user to introduce the data of the new item or edit the data of an existing item.

Its main components are:

- `SearchBar`: the search bar of the table. It allows the user to search for a specific value in the table.
- `FilterButtons`: the filter buttons of the table. It allows the user to filter the table by one or more categories.
- `AssetFlyout`: the flyout that is shown when the user clicks on an asset in the table. It shows the details of the asset and allows the user to edit or add a new asset.
- `FormTabs`: the tabs that are shown in the `AssetFlyout`. It allows the user to switch between multiple asset forms.
- `AssetForm`: the form that is shown in the `AssetFlyout`. It allows the user to edit or add a new asset. It handles the form validation and mutations to the backend.
- `FormFields`: the fields of the `AssetForm`. It handles the dynamic rendering of the `InputField` components based on the schema of the table.
- `InputField`: It renders the appropriate input field depending on the type of the field provided.
- `LegsInput`: It is a special type of input that is used when adding o editing the legs of a driver.

The `DataViewGrid` uses the schemas defined in `src/schemas` to render the table and the forms with the appropriate fields/columns.

The forms values are cached using the localStorage. This allows the user to keep the values of the forms when navigating between pages. The cache can be cleared using the "Reset" button in the upper part of the flyout.

There is a special input for assets fields. It is the `AddableSelect` component. It allows the user to add new values to the selectable. It fetches the values from the backend in real time. When the user clicks on the "Add" button, the `AssetForm` is replaced by a new form to add the new value, then the new value is added to the database and the select input is updated, then the form is replaced by the original form again. This is done to avoid the user to leave the flyout to add a new value needed in the current form.

So for example if the user wants to add a new product, with an exchange that doesn't exist yet he clicks on the "Add new exchange" button in the exchange selectable input, then a new form is shown to add the new exchange. When the user clicks on the "Add" button, the new exchange is added to the database and the selectable and the form is replaced by the product form againg.
(schemas)=

## Schemas - `src/schemas`

The schemas are the definitions of the fields of each of the database tables. They are used to render the tables and forms fields. They are also used to validate the data of the forms when submitting them.

There is a schema file for each table defining the fields. The available properties of the fields are:

- `id`: the id of the field. It is the actual name of the column in the database,
- `display`: The name displayed in the header of the table or in the form. Is is the name the user sees.
- `inputType`: The input type to use in the form generation. Available values are: "asset", "text", "number", "date", "select", "list", "boolean", "legs", "datetime". These are defined in `src/utils.js`
- `initialWidth`: The initial width of the column in the table.
- `validator`: The [yup](https://github.com/jquense/yup) validator to use to validate the field. Usually validators are defined in `src/validators.js` to be reused.
- `isPrimaryKey`: A boolean indicating if the field is the primary key of the table. This is needed because the are assets that have a composite primary key like products, which have the primary key (product, product_type).
- `options`: The options of the field. It is used only with the "select" inputType.
- `grow`: A number indicating the width of the column in the form.
- `assetType`: The type of the asset. It is used in the "asset" input type. And represent the endpoint of the backend to fetch the data for this input. Ex: "products", "exchanges". This field is mandatory for the "asset" input type.
- `assetId`: The field of the asset that is used as the id of the asset. It is used in the "asset" input type. Usually is the same as the id field. But in some cases it is not. Ex: "exchange" field of the product table. The id of the exchange is the "mic" field. This field is mandatory for the "asset" input type.

```{note}
The inputType ``asset`` is a special field type use by fields that are references to other tables. For example, the "exchange" field of the product table is a reference to an element of the Exchanges table.
```

In the `src/schemas/validators.jsx` file there are the validators used by the schemas. They are defined using the yup library and they are defined separately to be reused in multiple schemas.

Also, in the `src/schemas/utils.js` file there are the utils functions related with schemas. Here there is the function to use to get the properly schema given a asset type for example.
(services)=

## Services - `src/services`

The services are the ones that handle the communication with the backend and the authentication server. They are used by the components to fetch the data from the backend using the REST API and the axios library.

These services are used from multiple places of the app. They are couple of static classes with methods to perform the requests to the backend. The error handling is done via react-query so we don't need to catch the errors in the services functions.

This classes and methods are not used directly in the components. They are used via react-query hooks (useQuery, useInfinityQuery, useMutation). This library allows to fetch data from the backend and cache it in the frontend. It also handles the loading and error states of the requests. See more info in the [react-query documentation](https://tanstack.com/query/v3/docs/react/overview).

The services are:

- `services/AuthService.js`: the service that handles the authentication with the authentication server.
- `services/APIService.js`: the service that handles the communication with the Data API.
  (providers)=

## Providers - `src/providers`

In this folder there are the providers and hooks of the app. They provide utility functions to the entire application.

- `ToastProvider`: the provider that handles the toast notifications of the app. It uses the EUI toast components to render the notifications.
- `useToast`: the hook that allows to use the toast notifications in the app.
- `ThemeProvider`: the provider that handles the theme of the app. It uses the EUI theme provider and provide some utility functions to get colors. Used for example for tag colors. In this file there is also the overrides of the EUI theme like the primary color to use.
- `useTheme`: the hook that allows to use the theme in the app.
