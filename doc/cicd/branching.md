# Intro #

This section describes the approach for using GIT. The main purpose is to outline the branching model that will support
multiple deployment scenarios. The model must be:

- simple to apply (minimal number of rules to follow)
- fully extensible (can encompass other VCS model e.g. GitFlow or GitHubFlow)
- capable of storing the information on deployments alongside the source control history

# Rules #

There are two classes of branches:

1. the branches we can deploy to all environments, including production
2. the branches we can only deploy to lower environments, excluding production

The below rules apply only for production deployments

1. Deploy from the particular commit
2. Move the PROD branch to the commit
3. Tag the commit you deployed from with additional metadata (environment, version)
4. Merge the branch you deploy from to all other opened branches that you are willing to deploy from in the future (
   second class of branches)

Ad.2 PROD branch indicates the latest commit that was successfully deploy to production

Ad.3 Tags with metadata carries the information necessary to recreate the deployments' history from commits' history.
Tags can be added by deployment tools when the deployment process if finished. Tags can also be added manually or by any
3rd part tool - in this case they can trigger a deployment process.

Ad.4 That's the most important step in this model. It ensure that the PROD branch can be always moved forward without
any merge commits. In this way we're free to deploy from any branch that we merged from the current branch.

> When we apply the above rules, the set of all git commits right after the deployment is
> a [lattice](https://en.wikipedia.org/wiki/Total_order). If commits ca and cb, such that ca <= cb and both belong to
> the [partially ordered set](https://en.wikipedia.org/wiki/Partially_ordered_set) C which contains all Git commits in the
> repository, then for each two-elements subsets S of C, [sup{S}](https://en.wikipedia.org/wiki/Infimum_and_supremum)
> and [inf{S}](https://en.wikipedia.org/wiki/Infimum_and_supremum) always exist.

> Subset of all deployment commits (those where the prod branch stood) form
> a [linear order](https://en.wikipedia.org/wiki/Total_order).

# Supported scenarios #

This section present several example of how the model rules support the most popular deployment scenarios.

## GitHub Flow ##

The GitHub flow is a workflow designed to work well with Git and GitHub. It focuses on branching around features and
makes it possible for teams to experiment freely, and make deployments regularly.

The idea is to create a dedicated branch whenever we need to work on a new feature. There can be multiple feature
branches opened in parallel.

![GitHubFlow](cicd-github.drawio.svg)

Each of the feature branches can be deployed to lower environments whenever it's needed. It's required to tag the commit
we deploy from with additional metadata (e.g. version, environment etc.)

When the feature is developed and properly verified, we can merge it to the **dev** branch. The **dev** branch is a
class 1 branch which means we can deploy from it to production environment. We apply the same principles for commit
tagging (Rule #2).

> Because there are no other opened class 1 branches (the branches we can deploy to production) we don't need to apply
> Rules #4. Only Rule #3 is applicable in this case.

## Hotfix ##

Starting point: we've already deployed v.1.1.0 to PROD environment from the **dev** branch. That's indicated by the *
*prod** branch attached to the commit we deployed from (tha latest commit marked in yellow).

![Hotfix-Start](cicd-hotfix-start.drawio.svg)

We proceed with further development on the **dev** branch. In the meantime, we discover a severe bug in v.1.1.0 that's
affecting production environment - we need to develop and release the hotfix immediately!

Firstly, we create a separate branch from the **prod** branch. Let's call it **urgent-hotfix** branch. When the
development is completed, we can deploy the hotfix to **TEST** and **PROD** environments consecutively. That's indicated
by applying the following rules:

- Rule #2: **prod** branch being moved to the latest commit in the **urgent-hotfix** branch
- Rule #3: added tags with deployment metadata (rule #3)
- Rule #4: **urgent-hotfix** is merged to the active **dev** branch.

> If we skipped the rule #4, it would be impossible to "slide" the **prod** branch any **dev** commit that might want to
> deploy from in the future.

> We don't need to merge our hotfix branch to feature branches, if there are any. Feature branches are considered Class
> 2 branches - we cannot deploy from them to production environment.

![Hotfix-End](cicd-hotfix-end.drawio.svg)

