# Misc Research

## Data Flow

- **Active** windows are those that have buffered data.
- **Bundles** are units of committment (that is, retry logic) in the Java SDK.

### Triggering etc. in Java SDK
- Execute a bundle
- Update watermarks for next execution

### Triggering etc. in Elixir SDK

## Reducing Transforms
Reducing transforms are those that do not just map elements, e.g. GBK. After the execution of the transform proper, the output is bufferred and processed by a `ReduceFn` which handles triggering of windows, etc. Specifically, in the Java SDK a `ReduceFnRunner`'s responsibilities include:

- Tracking the windows that are active (have buffered data) as elements arrive and triggers are fired.
- Holding the watermark based on the timestamps of elements in a pane and releasing it when the trigger fires.
- Calling the appropriate callbacks on `ReduceFn` based on trigger execution, timer firings, etc, and providing appropriate contexts to the `ReduceFn` for actions such as output.
- Scheduling garbage collection of state associated with a specific window, and making that happen when the appropriate timer fires.

The behaviour of a `ReduceFnRunner` also heavily depends on the `WindowingStrategy` in effect. This affects triggering, window merging, active window tracking/expiry, emission of final panes and the mode of late panes (discarding/accumulating).

## Interesting Miscellanea
```
org/apache/beam/runners/core/ReduceFnRunner.java:238  

// Note this may incur I/O to load persisted window set data.  
this.activeWindows = createActiveWindowSet();
```

## Classes to keep track of
- `org.apache.beam.runners.core.ReduceFnRunner`
- `org.apache.beam.runners.core.WatermarkHold`
- `org.apache.beam.runners.core.ActiveWindowSet`
- `org.apache.beam.runners.direct.WatermarkManager`

## Gotchas
- `(value, [window1, window2])` actually is two values—one per window.


## Design Docs

- [Lateness (and Panes) in Apache Beam](https://docs.google.com/document/d/12r7frmxNickxB5tbpuEh_n35_IJeVZn1peOrBrhhP6Y/edit)
- [State and Timers for DoFn in Apache Beam](https://docs.google.com/document/d/1zf9TxIOsZf_fz86TGaiAQqdNI5OO7Sc6qFsxZlBAMiA/edit)
- [A New DoFn](https://docs.google.com/document/d/1ClmQ6LqdnfseRzeSw3SL68DAO1f8jsWBL2FfzWErlbw/edit#)
- [Presentation on Beam (old)](https://docs.google.com/presentation/d/1uTb7dx4-Y2OM_B0_3XF_whwAL2FlDTTuq2QzP9sJ4Mg/edit#slide=id.g127d614316_41_113)
