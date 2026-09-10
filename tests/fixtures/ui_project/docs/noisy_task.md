# noisy_task

Detailed fixture documentation for the task the UI tests walk through.

## Behaviour

The task prints one ordinary line, one stderr line, and raises two warnings.
Its `mode` argument is supplied by the profile.

## Raw markup is data, not markup

This line contains a deliberate <script>alert("xss")</script> and a
<img src=x onerror="alert('xss')"> so that the renderer's `{"html": False}`
setting is exercised by a real file: the walkthrough must show these as text.
