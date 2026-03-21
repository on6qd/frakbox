#!/bin/bash
cd "$(dirname "$0")"
tail -f $(ls -t logs/*.log | head -1)
