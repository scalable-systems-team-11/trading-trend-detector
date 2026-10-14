# A simple log to keep track of what we have done and when, and what decisions where made and why.
## Keep this really straight forward but try to remember to fill it in so we can use this when writing report later

## Log
- 2x.9 (All) First meeting planning architecture
- 2x.9 (All) Team decided to try and replicate "real time" streaming in some way, although we might have to use smaller dataset/slower speed depending on hardware
- 28.9 (Jens) Using AI to set up project structure, and trying to create a data loader. Using a smaller subset of one days data.
- 29.9 (Jens) Setting it up to run in docker, and adding a minimal TCP server to recieve (will be replaced with actual handler later). Testing that this MVP works, writing a readme etc
- 6.10 (Yuhang, AI-assisted candidate implementation) Added the stream processor: per-symbol 5-minute windows, EMA38/EMA100 starting from 0 and BUY/SELL crossover detection, connected to the receiver. Added unit and pipeline tests. Details and open decisions in docs/stream_processor.md
- 6.10 (Yuhang, AI-assisted local follow-up) Fixed replayer `--end` stopping early on out-of-order/placeholder rows; it now skips them and keeps scanning (slower, reads the whole file)
- 6.10 (Yuhang, AI-assisted local follow-up) Index rows have no Trading date in the real data, so for those we use the system Date instead (same-day assumption, needs to be mentioned in the report)
- 6.10 (Yuhang) First real-data run on Monday 08:00-09:00, results in docs/windows_validation_2026-10-06.md. Still to do: Docker test, throughput/latency measurements, checking SELL signals on real data
- 13.10 (Jens) Cleanup: shortened this log and the README back to original format
- 14.10 (Jens) Tried some full day runs on Monday data. Got varyings throughput speeds, from 20k to 50k events/seconds. According to docker stats, bottleneck is currently the replayer (CPU usage maxed out while conusmer only uses ~60% CPU). Made some small fixes: replayer exiting before all data was sent, added .env, output by datetime etc. Also: realised that about 1M equity trades currently get dropped for missing trading date, maybe we need to use the system date here?
