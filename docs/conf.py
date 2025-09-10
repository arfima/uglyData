# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))


# -- Project information -----------------------------------------------------

project = "DataViewer"
copyright = "2024, Arfima Trading"
author = "Arfima Trading"

# The full version, including alpha/beta/rc tags
release = "1.0.0"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.linkcode",
    "myst_parser",
    "sphinx_design",
    "sphinx_copybutton",
]

# To use sphinx-design grid
myst_enable_extensions = ["colon_fence"]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "README.md"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "pydata_sphinx_theme"

html_theme_options = {
    "use_edit_page_button": True,
    "icon_links": [
        {
            "name": "GitLab",
            "url": "https://git.arfima.com/arfima/arfima/uglydata/-/tree/main/frontend",
            "icon": "fab fa-gitlab-square",
        },
    ],
    # "navbar_end": ["navbar-icon-links", "theme_switcher"],
    "logo": {
        # "text": f'<p style="font-family: "Roboto"; font-weight: bold; font-size:24px">{project} {release}</p>', # noqa
        "text": f"""
                <link rel="preconnect" href="https://fonts.googleapis.com">
                <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
                <link href="https://fonts.googleapis.com/css2?family=Montserrat" rel="stylesheet"> 
                <p style="font-family: 'Montserrat', sans-serif; font-weight: 900; font-size:24px">{project}</p>
        """  # noqa
    },
    "pygment_dark_style": "material",
    "show_toc_level": 2,
}


# For Edit Button
html_context = {
    "gitlab_url": "https://git.arfima.com",
    "gitlab_user": "arfima",
    "gitlab_repo": "arfima/uglyData",
    "gitlab_version": "main",
    "doc_path": "docs",
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
html_css_files = ["custom.css"]
html_logo = "_static/images/arfima-logo.png"
html_favicon = "_static/images/favicon.png"
html_title = f"{project} {release}"


# Sphinx-js configuration
js_source_path = "../src"
# jsdoc_config_path='../jsdoc.conf.json'
primary_domain = "js"


def linkcode_resolve(domain, info):
    if domain != "js":
        return None
    filepath = f'{info["object"]}.{domain}'
    return f"https://git.arfima.com/arfima/arfima/uglydata/-/tree/main/frontend/src/{filepath}"
