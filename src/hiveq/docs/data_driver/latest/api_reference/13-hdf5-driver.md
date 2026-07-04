## HDF5 Data driver

By default compression is enabled (zlib, level 9). Change it per target section:

```ini
[DailyBarsHDF5]
transport         = HDF5
store             = hdf5/dd.h5
key               = {sym}-{date}
enableCompression = True
compression       = zlib
compression_level = 9
```

> Default pandas HDF5 stores can be larger than other formats; choose a
> compression level/algorithm that suits your data.

