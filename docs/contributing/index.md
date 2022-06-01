# Contributing to Maestro

To start develeloping on Maestro, first create a Fork from the [Maestro repo](https://github.com/estudio89/maestro-python) on Github.

Then clone your fork. The clone command will look like this, with your Github username instead of YOUR-USERNAME:

    git clone https://github.com/YOUR-USERNAME/maestro-python

## Testing

To run the tests after cloning the repository, first create a virtual environment:

    # Setup virtual environment
    python3 -m venv env
    source env/bin/activate
    pip install -r requirements.txt
    pip install -r requirements-docs.txt

    # Run the tests
    ./run_tests.sh

### Test options

Run a subset of the tests:

    ./run_tests.sh tests/in_memory/
    ./run_tests.sh tests/django/
    ./run_tests.sh tests/firestore/
    ./run_tests.sh tests/mongo/

### Testing the different backends

#### Firestore
In order to run the firestore tests, its necessary to run the firestore emulator. For that, you'll need to have node >= 12 installed. For instructions on how to install node, see the [node documentation](https://nodejs.org/en/download/).

After node is installed, you need the firebase CLI. You can see instructions on how to install it in the [Firebase documentation](https://firebase.google.com/docs/cli#install-cli-mac-linux).

After the CLI is installed, you'll need to log into your Firebase account. To do that, run the command below:

    firebase login

After that, you'll need to create a firestore project and download its `serviceAccountKey.json` file (it should be placed under `tests/firestore/firebase-project/`). You can find more instructions on how to do that in the [Firebase docs](https://firebase.google.com/docs/admin/setup#set-up-project-and-service-account).

Then, you'll need to install the firebase test project dependencies. To do that, run the commands below:

    cd tests/firestore/firebase-project/functions
    npm install

Then, run the following command to start the emulator:

    npm run serve

After doing that, the CLI will print out instructions with an address that will let you access the emulator.

Keep the emulator running and in a separate terminal window, you'll be able to run all the firestore tests:

    ./run_tests.sh tests/firestore/

### MongoDB

In order to run all MongoDB tests, you'll need to run it locally on your machine. The easiest way to do that is by using [Docker](https://docs.docker.com/engine/install/ubuntu/).

After docker is installed, first modify the permissions for the sample mongo key file for it to work correctly with docker:

    sudo chown 999:999 mongo-keyfile

Then you can simply run the `dev_mongodb.sh` script located in the root folder. This will run a MongoDB container in the background.

Then, running the tests is as easy as:

    ./run_tests.sh tests/mongo/

As a tip, use [MongoDB Compass](https://www.mongodb.com/products/compass) to visualize the data.

## Documentation

The documentation for Maestro is built from Markdown source files in the docs directory.

### Building the documentation

To build the documentation, install MkDocs with pip install mkdocs and then run the following command.

    mkdocs build

This will build the documentation into the `site` directory.

You can build he documantion and open a preview in a browser window by usint the `serve` command.

    mkdocs serve

By default, the documentation will be available at http://localhost:8000. If you need a different address or port, you can pass in the `-a` option:

    mkdocs server -a 0.0.0.0:3030




