from __future__ import annotations

import importlib
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import click
import typer
import typer.core
from rich.console import Console
from rich.tree import Tree


def walk_commands(node: click.Group | click.Command, tree: Tree) -> None:
    """Recursively walk click commands and add them to the rich tree."""
    if isinstance(node, click.Group):
        ctx = click.Context(node)
        for cmd_name in node.list_commands(ctx):
            # get_command handles lazy-loading logic automatically
            cmd = node.get_command(ctx, cmd_name)
            if cmd:
                help_text = cmd.help.split("\n")[0] if cmd.help else ""
                # Truncate help text if too long
                if len(help_text) > 60:
                    help_text = help_text[:57] + "..."

                branch = tree.add(f"[bold cyan]{cmd_name}[/] [dim]— {help_text}[/]")
                walk_commands(cmd, branch)


def display_tree_callback(ctx: typer.Context, value: bool) -> None:
    """Typer callback to display the command tree from the current node down."""
    if value and not ctx.resilient_parsing:
        console = Console()
        # Find the current command
        command = ctx.command

        # Determine the name for the root of the tree
        # Use the full command path if possible
        full_path: list[str | None] = []
        p: click.Context | None = ctx
        while p:
            full_path.insert(0, p.info_name)
            p = p.parent

        root_label = " ".join(filter(None, full_path)) or "PSETI"
        root_tree = Tree(f"[bold reverse] {root_label} Structure [/]")
        walk_commands(command, root_tree)
        console.print("\n", root_tree, "\n")
        raise typer.Exit()


# Trick to ensure subclassing works across different import states
if TYPE_CHECKING:
    from typer.core import TyperGroup
else:
    import typer.core

    TyperGroup = typer.core.TyperGroup


class BaseLazyGroup(TyperGroup):
    """
    Base class for lazy-loading Click Groups in Typer.
    """

    def __init__(
        self,
        *args: Any,
        lazy_mapping: dict[str, tuple[str, str, str]] | None = None,
        command_order: list[str] | None = None,
        path_injector: Callable[[str], None] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.lazy_mapping = lazy_mapping or {}
        self.command_order = command_order
        self.path_injector = path_injector

    def list_commands(self, ctx: click.Context) -> list[str]:
        """Return the list of commands in the desired order."""
        base_cmds = super().list_commands(ctx)
        # Use a list for all_cmds to preserve order from lazy_mapping keys
        # We start with base_cmds (usually empty for PSETI root) then lazy_mapping keys
        all_cmds_set = set(base_cmds) | set(self.lazy_mapping.keys())

        # Determine the default order (Base commands then Lazy commands in definition order)
        default_order = []
        for cmd in base_cmds:
            if cmd not in default_order:
                default_order.append(cmd)
        for cmd in self.lazy_mapping:
            if cmd not in default_order:
                default_order.append(cmd)

        if self.command_order:
            # Filter explicit order to only include commands that actually exist
            ordered = [c for c in self.command_order if c in all_cmds_set]
            # Append anything else that wasn't in the explicit order (alphabetically sorted)
            remaining = sorted([c for c in all_cmds_set if c not in ordered])
            return ordered + remaining

        return default_order

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        # 1. Try standard command
        cmd = super().get_command(ctx, cmd_name)
        if cmd is not None:
            return cmd

        # 2. Try lazy command
        if cmd_name in self.lazy_mapping:
            module_path, attr_name, help_str = self.lazy_mapping[cmd_name]

            # Optimization: Skip loading if we just want the top-level help
            is_help_mode = any(arg in sys.argv for arg in ["--help", "-h"])
            is_targeting_this = cmd_name in sys.argv
            if is_help_mode and not is_targeting_this and not getattr(ctx, "resilient_parsing", False):
                return click.Command(cmd_name, help=help_str)

            # Inject paths if needed (e.g. for control/src/ci or grpc/src)
            if self.path_injector:
                self.path_injector(cmd_name)

            try:
                mod = importlib.import_module(module_path)
                if not hasattr(mod, attr_name):
                    return None

                obj = getattr(mod, attr_name)

                if isinstance(obj, typer.Typer):
                    click_cmd = typer.main.get_command(obj)
                else:
                    # Wrap bare function in a Typer app
                    temp_app = typer.Typer()
                    temp_app.command(name=cmd_name, help=help_str)(obj)
                    click_cmd = typer.main.get_command(temp_app)

                # Promote single-command groups to actual commands
                if isinstance(click_cmd, click.Group):
                    command_names = click_cmd.list_commands(ctx)
                    if len(command_names) == 1:
                        actual_cmd = click_cmd.get_command(ctx, command_names[0])
                        if actual_cmd:
                            if not actual_cmd.help:
                                actual_cmd.help = click_cmd.help
                            actual_cmd.name = cmd_name
                            return actual_cmd

                click_cmd.name = cmd_name
                if not click_cmd.help:
                    click_cmd.help = help_str
                return click_cmd

            except Exception as e:
                click.secho(f"Error loading command '{cmd_name}': {e}", fg="red", err=True)
                return None
        return None
