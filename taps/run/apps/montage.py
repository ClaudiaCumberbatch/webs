from __future__ import annotations

import pathlib

from pydantic import Field
from pydantic import field_validator

from taps.app import App
from taps.app import AppConfig
from taps.run.apps.registry import register_app


@register_app(name='montage')
class MontageConfig(AppConfig):
    """Montage application configuration."""

    img_folder: str = Field(description='input images folder path')
    img_tbl: str = Field(
        'Kimages.tbl',
        description='input image table filename',
    )
    img_hdr: str = Field(
        'Kimages.hdr',
        description='header filename for input images',
    )
    output_dir: str = Field(
        'data',
        description=(
            'output folder path for intermediate and final data '
            '(relative to run directory)'
        ),
    )

    @field_validator('img_folder', mode='before')
    @classmethod
    def _resolve_paths(cls, root: str) -> str:
        return str(pathlib.Path(root).resolve())

    def create_app(self) -> App:
        """Create an application instance from the config."""
        from taps.apps.montage import MontageApp

        return MontageApp(
            img_folder=pathlib.Path(self.img_folder),
            img_tbl=self.img_tbl,
            img_hdr=self.img_hdr,
            output_dir=self.output_dir,
        )