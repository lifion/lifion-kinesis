workflow "Checks & Publish" {
  on = "push"
  resolves = ["Publish"]
}

action "Install" {
  uses = "actions/npm@master"
  args = "ci"
}

action "Lint" {
  uses = "actions/npm@master"
  needs = ["Install"]
  args = "run eslint"
}

action "Test" {
  uses = "actions/npm@master"
  needs = ["Install"]
  args = "test"
  secrets = ["CODECOV_TOKEN"]
}

action "Master" {
  uses = "actions/bin/filter@master"
  needs = ["Lint", "Test"]
  args = "branch master"
}

action "Publish" {
  uses = "actions/npm@master"
  args = "publish"
  secrets = ["NPM_AUTH_TOKEN"]
  needs = ["Master"]
}
