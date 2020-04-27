# RoLaGuard Community Edition

## Data collectors

This repository contains the source code of the RoLaGuard data collectors. This component is the responsible of gathering packets from different sources and send it to the packet writer, which inserts the packets in the system database for further analysis.

To access the main project with instructions to easily run the rolaguard locally visit the [RoLaGuard](https://github.com/Argeniss-Software/rolaguard) repository. For contributions, please visit the [CONTRIBUTIONS](https://github.com/Argeniss-Software/rolaguard/blob/master/CONTRIBUTIONS.md) file
​

### Build the docker image

First build a docker image locally:

```bash
docker build -t rolaguard-data-collectors
```
