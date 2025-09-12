"""Template management for mail notifications."""

import os
from pathlib import Path
from typing import Any, Dict, Optional
from jinja2 import Environment, FileSystemLoader, Template, select_autoescape

from .config import MailTemplate, MailStatus
from .exceptions import TemplateError


class TemplateManager:
    """Manages email templates using Jinja2."""

    def __init__(self, templates_dir: Path):
        """
        Initialize template manager.

        Args:
            templates_dir: Directory containing email templates
        """
        if not templates_dir.exists():
            raise ValueError(f"Templates directory not found: {templates_dir}")

        self.templates_dir = templates_dir
        self.env = Environment(
            loader=FileSystemLoader(str(templates_dir)),
            autoescape=select_autoescape(["html", "xml"]),
            trim_blocks=True,
            lstrip_blocks=True,
        )

        # Register custom filters
        self.register_custom_filters()

        # Load and validate all templates
        self._templates: Dict[str, Template] = {}
        self._load_templates()

    def register_custom_filters(self) -> None:
        """Register custom Jinja2 filters."""
        self.env.filters["format_date"] = lambda d, fmt="%Y-%m-%d %H:%M:%S": d.strftime(
            fmt
        )
        self.env.filters["truncate_text"] = lambda t, n=100: (
            t[:n] + "..." if len(t) > n else t
        )

    def _load_templates(self) -> None:
        """Load all templates from the templates directory."""
        template_files = self.templates_dir.glob("*.html")
        for template_file in template_files:
            try:
                template_name = template_file.stem
                self._templates[template_name] = self.env.get_template(
                    template_file.name
                )
            except Exception as e:
                raise TemplateError(f"Error loading template {template_file}: {e}")

    def render(
        self,
        template_name: str,
        context: Dict[str, Any],
        default_context: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Render a template with the given context.

        Args:
            template_name: Name of the template to render
            context: Context variables for template rendering
            default_context: Default context to merge with provided context

        Returns:
            str: Rendered template

        Raises:
            TemplateError: If template cannot be found or rendered
        """
        try:
            # Merge default context with provided context
            render_context = {}
            if default_context:
                render_context.update(default_context)
            render_context.update(context)

            # Get template
            if template_name not in self._templates:
                self._load_templates()  # Reload templates in case new ones were added

            template = self._templates.get(template_name)
            if not template:
                raise TemplateError(f"Template not found: {template_name}")

            # Render template
            return template.render(**render_context)

        except Exception as e:
            raise TemplateError(f"Error rendering template {template_name}: {e}")

    def get_default_template(self, status: MailStatus) -> MailTemplate:
        """Get default template configuration for a mail status."""
        templates = {
            MailStatus.START: MailTemplate(
                name="pipeline_start",
                subject="[START] Pipeline Execution Started",
                path=self.templates_dir / "pipeline_start.html",
                default_context={"status": "started"},
            ),
            MailStatus.SUCCESS: MailTemplate(
                name="pipeline_success",
                subject="[SUCCESS] Pipeline Execution Completed",
                path=self.templates_dir / "pipeline_success.html",
                default_context={"status": "completed"},
            ),
            MailStatus.ERROR: MailTemplate(
                name="pipeline_error",
                subject="[ERROR] Pipeline Execution Failed",
                path=self.templates_dir / "pipeline_error.html",
                default_context={"status": "failed"},
            ),
            MailStatus.WARNING: MailTemplate(
                name="pipeline_warning",
                subject="[WARNING] Pipeline Execution Warning",
                path=self.templates_dir / "pipeline_warning.html",
                default_context={"status": "warning"},
            ),
        }

        template = templates.get(status)
        if not template:
            raise TemplateError(f"No default template found for status: {status}")
        return template
