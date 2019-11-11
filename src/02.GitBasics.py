# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,../src//py
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.2.4
#   kernelspec:
#     display_name: big-data
#     language: python
#     name: big-data
# ---

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Git
#
# Git is a free and open source distributed version control system designed to handle everything from small to very large projects with speed and efficiency.
#
# Official website https://git-scm.com

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # GitHub
#
# - Web-based hosting service for version control using Git. 
# - Offers all of the distributed version control and source code management (SCM) functionality of Git as well as adding its own features. 
# - Provides access control and several collaboration features such as bug tracking, feature requests, task management, and wikis for every project.
# - Github is the largest host of source code in the world.

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # About SCM
#
# - Records changes to a file or set of files over time.
# - You can recall specific versions later.
# - You can use it with nearly any type of file on a computer.
# - This is the better way to collaborate on the same document.
# - Every change is committed with an author and a date.
# - Figures are downloaded from [Pro Git book](http://git-scm.com/book).
# - "Become a git guru" tutorial (https://www.atlassian.com/git/tutorials).

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Local Version Control System
#
# <img src="images/local.png" alt="rcs" width="400px"/>
#
# - Collaboration is not really possible.

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Distributed Version Control Systems
#
# <img src="images/git.png" alt="git" width="300px"/>
#
# - Clients fully mirror the repository.
# - You can collaborate with different groups of people in different ways simultaneously within the same project.
# - No need of network connection.
# - Multiple backups.
#  
#  

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Configure Git
#
# Settings are saved on the computer for all your git repositories.
#
# ```
# git config --global user.name “Prenom Nom"
# git config --global user.email “prenom.nom@univ-rennes2.fr"
# ```

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Cloning the repository
#
# ```bash
# git clone ssh://svmass2/git/big-data.git
# ```
# or
# ```bash
# git clone https://github.com/pnavaro/big-data.git
# ```

# + {"slideshow": {"slide_type": "fragment"}, "cell_type": "markdown"}
# # To save your work locally create a branch
#
# ```bash
# git checkout -b my_branch
# ```
# -

# # Four File status in the repository
#   
# <img src="images/18333fig0201-tn.png" alt="git" width="450px"/>

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Git Workflow
#
# <img src="images/four_stages.png" alt="git" width="150px"/>

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Locally saving your modifications
#
# Get your files status

# + {"slideshow": {"slide_type": "slide"}, "language": "bash"}
# git status

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Add the modified file to the index
#
# ```bash
# git add your_notebook_copy.ipynb
# ```

# + {"slideshow": {"slide_type": "fragment"}, "cell_type": "markdown"}
# Checking which files are ready to be committed.

# + {"slideshow": {"slide_type": "fragment"}, "language": "bash"}
# git status

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# Now save your work, the branch is local.
#
# ```bash
# git commit -m 'Some modifications'
# ```

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Updating from the Repository
#
# If the master branch has changed. To get all new updates :

# + {"attributes": {"classes": ["git"], "id": ""}, "slideshow": {"slide_type": "fragment"}, "language": "bash"}
# git pull origin master

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Solve conflicts
#
# - If you have some conflicts, no problem just do :
#
# ```
# git checkout notebook.ipynb
# ```
#
# It will give you back the original version of notebook.ipynb
#
# - If you have big troubles, you can do
#
# ```
# git reset --hard
# ```
#
# Be careful with this last command, you remove uncommited changes.
#

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Git through IDE
#
# - Install bash-completion and source git-prompt.sh.
# - Use Gui tools:
# 	- [GitHub Desktop](https://desktop.github.com/)
# 	- [Sourcetree](https://fr.atlassian.com/software/sourcetree)
# 	- [GitKraken](https://www.gitkraken.com/)
# - VCS plugin of IDE
# 	- [RStudio](https://www.rstudio.com/)
# 	- [Eclipse](https://www.eclipse.org/downloads/)
# 	- [JetBrains](https://www.jetbrains.com/)
