# Axon extension dbscheduler

This project aims to provide a deadline manager for the AxonFramework. This can be a replacement of the (only) other
implementation based on Quartz.

### Build with

* axon framework 4.5
* dbscheduler 10.3

## Usage

1. Add <https://jitpack.io> as a repository to your project
2. Declare this module as a dependency

   with maven:
   ```xml
   <dependency>
     <groupId>com.github.SanderSmee</groupId>
     <artifactId>axon-extension-dbscheduler</artifactId>
     <version>master-SNAPSHOT</version>
   </dependency>
   ```

   with gradle:
   ```gradle
   dependencies {
     implementation 'com.github.SanderSmee:axon-extension-dbscheduler:master-SNAPSHOT'
   }
   ```
3. Configure axon framework to use `DbSchedulerDealineManager` instead of `QuartzDeadlineManager`. See
   the [axon reference guide](https://docs.axoniq.io/reference-guide/) how to configure a deadline manager.

## Roadmap

This is an experimental project. No production tests or validation has been done. There is no publication of this module
to maven central.

## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any
contributions you make are **greatly appreciated**.

1. Fork the Project
2. Create your Feature Branch (git checkout -b feature/AmazingFeature)
3. Commit your Changes (git commit -m 'Add some AmazingFeature')
4. Push to the Branch (git push origin feature/AmazingFeature)
5. Open a Pull Request

## License

Distributed under the Apache v2 License. See `LICENSE` for more information.

## Acknowledgements

* https://github.com/AxonFramework/AxonFramework
* https://github.com/kagkarlsson/db-scheduler
