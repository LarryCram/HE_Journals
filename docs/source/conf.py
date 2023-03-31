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
import os
import sys
sys.path.insert(0, os.path.abspath('../..'))


# -- Project information -----------------------------------------------------

project = 'OpenResearchEvaluation'
copyright = '2023, Lawrence Cram'
author = 'Lawrence Cram'

# The full version, including alpha/beta/rc tags
release = '0.0.1'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosectionlabel',
    'sphinx.ext.autosummary',
    'sphinx.ext.coverage',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    ]
# sphinx.ext.doctest – Test snippets in the documentation
# sphinx.ext.duration – Measure durations of Sphinx processing
# sphinx.ext.extlinks – Markup to shorten external links
# sphinx.ext.githubpages – Publish HTML docs in GitHub Pages
# sphinx.ext.graphviz – Add Graphviz graphs
# sphinx.ext.ifconfig – Include content based on configuration
# sphinx.ext.imgconverter – A reference image converter using Imagemagick
# sphinx.ext.inheritance_diagram – Include inheritance diagrams
# sphinx.ext.intersphinx – Link to other projects’ documentation
# sphinx.ext.linkcode – Add external links to source code
# Math support for HTML outputs in Sphinx
# sphinx.ext.todo – Support for todo items

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'alabaster'
# html_theme = 'sphinx_rtd_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
