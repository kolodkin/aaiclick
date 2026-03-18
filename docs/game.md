Game Module
---

The `aaiclick.game` package defines game mechanics for a turn-based combat system with mage skills.

# Skills

**Implementation**: `aaiclick/game/skills.py` — see `MAGE_SKILLS`, `Skill`, `SkillEffect`, `SkillEffectType`

Skills are defined per level in `MAGE_SKILLS`. Each `Skill` has one or more `SkillEffect` entries describing what happens when the skill is cast.

## Mage Level 2 Skills

| Name           | Hebrew          | Effect                                      |
|----------------|-----------------|---------------------------------------------|
| Storm          | סופה            | `-2` dice penalty to opponent's attacks     |
| Dragon Breath  | נשימת דרקון     | Neutralizes `1` item of the opponent        |

# Combat

**Implementation**: `aaiclick/game/combat.py` — see `Player`, `apply_skill()`

```python
from aaiclick.game import Player, apply_skill, MAGE_SKILLS

opponent = Player(name="Opponent", dice_bonus=5, items=["Shield", "Sword"])

storm = next(s for s in MAGE_SKILLS[2] if s.name == "Storm")
apply_skill(storm, opponent)
# opponent.dice_bonus == 3

dragon_breath = next(s for s in MAGE_SKILLS[2] if s.name == "Dragon Breath")
apply_skill(dragon_breath, opponent)
# opponent.items == ["Shield"]
```

`apply_skill()` mutates the target `Player` in-place, applying all effects of the skill.

## Effect Types

| `SkillEffectType`  | Behavior                                              |
|--------------------|-------------------------------------------------------|
| `DICE_PENALTY`     | Subtracts `value` from `target.dice_bonus`            |
| `NEUTRALIZE_ITEM`  | Removes last `value` items from `target.items`        |

# Testing

- **Unit tests**: `aaiclick/game/test_skills.py` — skill data validation
- **E2E tests**: `aaiclick/game/test_e2e.py` — full combat scenarios
