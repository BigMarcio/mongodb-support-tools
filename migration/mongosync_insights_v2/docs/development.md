# Development Notes

This guide contains developer-oriented notes for local work on Mongosync Insights.

## Template Structure

Templates are located in `templates/` and are rendered with Flask `render_template()`.

Key templates include:

- `base_home.html` and `base_metrics.html`: shared layout and common UI structure
- `home.html`: landing/upload and entry points to monitoring
- `setup.html`: setup workflow (Quick Setup and Advanced modes)
- `metrics.html`: live monitoring dashboard
- `stream_logs.html`: log streaming view
- `error.html`: generic error display

## Local Development

Install dependencies and run:

```bash
cd migration/mongosync_insights
pip3 install -r requirements.txt
python3 mongosync_insights.py
```

## Testing Recommendations

- Validate setup flow for both modes (`/setup`)
- Validate live monitor refresh and endpoint handling
- Validate upload parsing for expected mongosync log formats
- Validate error pages for unreachable endpoint/invalid connection

## Documentation Organization

- `README.md` is the user entrypoint.
- `docs/configuration.md` holds environment-variable details.
- `docs/security.md` covers HTTPS and connection-string security behavior.
- `docs/features/connection-setup.md` tracks setup feature behavior and test checklist.

Historical docs are kept in `docs/archive/`.
