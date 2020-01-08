---
layout: post
title:  "DeepTrace: A Generic Framework for Time Series Forecasting"
subtitle: "Presented at IWANN 2019, Grann Canaria - Spain"
permalink:  "DeepTrace"
code: "https://github.com/adithya8/Deep-Trace"
link: "https://link.springer.com/chapter/10.1007/978-3-030-20521-8_12"
date:   2019-06-12 00:00:00 -0200
categories: jekyll update
---


We propose a generic framework for time-series forecasting called DeepTrace, which comprises of 5 model variants. These variants are constructed using two or more of three task specific components, namely, Convolutional Block, Recurrent Block and Linear Block, combined in a specific order. We also introduce a novel training methodology by using future contextual frames. However, these frames are dropped during the testing phase to verify the robustness of DeepTrace in real-world scenarios. We use an optimizer to offset the loss incurred due to the non-provision of future contextual frames. The genericness of the framework is tested by evaluating the performance on real-world time series datasets across diverse domains. We conducted substantial experiments that show the proposed framework outperforms the existing state-of-art methods.