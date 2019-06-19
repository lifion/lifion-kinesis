workflow "Build, lint and test" {
  resolves = ["Test"]
  on = "push"
}

action "Build" {
  uses = "actions/npm@59b64a598378f31e49cb76f27d6f3312b582f680"
  runs = "install"
}

action "Lint" {
  uses = "actions/npm@59b64a598378f31e49cb76f27d6f3312b582f680"
  runs = "run eslint"
  needs = ["Build"]
}

action "Test" {
  uses = "actions/npm@59b64a598378f31e49cb76f27d6f3312b582f680"
  runs = "test"
  needs = ["Lint"]
}
