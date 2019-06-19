workflow "Build, lint and test" {
  resolves = ["Test"]
  on = "push"
}

action "Build" {
  uses = "actions/npm@master"
  args = "install"
}

action "Lint" {
  uses = "actions/npm@master"
  needs = ["Build"]
  args = "run eslint"
}

action "Test" {
  uses = "actions/npm@master"
  needs = ["Lint"]
  args = "test"
  secrets = ["CODECOV_TOKEN"]
}
