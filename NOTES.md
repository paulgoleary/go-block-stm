
### TODO

* The paper mentions an optimization where tasks that fail from a dependency check violation will be recorded and not re-executed until the dependency is resolved. I've observed a behavior where if a task fails quickly with a read violation it will 'spin' and be re-executed rapidly over and over. This sometimes can reach thousands spurious execution until the dependent task clears. This optimization will likely mitigate this behavior and needs to be implemented. Right now I avoid this by making even failed test tasks wait their full duration.
* The scenario where an incarnation writes to a different output set is not handled at the moment, although it should be straightforward.
* Among other areas of incompleteness, output is currently not collected.
* Need to formalize logging.
* **LOTS** more testing!