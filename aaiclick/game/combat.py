from __future__ import annotations

from dataclasses import dataclass, field

from .skills import Skill, SkillEffectType


@dataclass
class Player:
    name: str
    dice_bonus: int = 0
    items: list[str] = field(default_factory=list)


def apply_skill(skill: Skill, target: Player) -> None:
    """Apply all effects of a skill to the target player in-place."""
    for effect in skill.effects:
        if effect.effect_type == SkillEffectType.DICE_PENALTY:
            target.dice_bonus -= effect.value
        elif effect.effect_type == SkillEffectType.NEUTRALIZE_ITEM:
            for _ in range(min(effect.value, len(target.items))):
                target.items.pop()
