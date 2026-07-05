import unittest
import importlib.util
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path

import torch

from sglang.test.ci.ci_register import register_amd_ci, register_cuda_ci


register_cuda_ci(est_time=9, stage="base-b", runner_config="1-gpu-small")
register_amd_ci(est_time=9, suite="stage-b-test-1-gpu-small-amd")

_LORA_OPS_PATH = (
    Path(__file__).parents[4]
    / "python"
    / "sglang"
    / "srt"
    / "lora"
    / "torch_ops"
    / "lora_ops.py"
)
_spec = importlib.util.spec_from_file_location("lora_ops_under_test", _LORA_OPS_PATH)
_lora_ops = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_lora_ops)


class TestLoRANoopRanks(unittest.TestCase):
    def test_rank_zero_skips_zero_weight_a_gemm_with_inf_input(self):
        inputs = torch.tensor([[float("inf"), 1.0]], dtype=torch.float32)
        weights = torch.zeros((1, 2, 2), dtype=torch.float32)

        output = _lora_ops.sgemm_lora_a_fwd(
            inputs=inputs,
            weights=weights,
            weight_indices=torch.tensor([0], dtype=torch.int32),
            seg_len_tensor=torch.tensor([1], dtype=torch.int32),
            lora_ranks=torch.tensor([0], dtype=torch.int32),
            scaling_tensor=torch.tensor([1.0], dtype=torch.float32),
        )

        self.assertTrue(torch.isfinite(output).all())
        torch.testing.assert_close(output, torch.zeros_like(output))

    def test_rank_zero_keeps_base_output_unchanged_with_inf_input(self):
        inputs = torch.tensor([[float("inf"), 1.0]], dtype=torch.float32)
        weights = torch.zeros((1, 2, 2), dtype=torch.float32)
        base_output = torch.tensor([[3.0, 4.0]], dtype=torch.float32)

        output = _lora_ops.sgemm_lora_b_fwd(
            inputs=inputs,
            weights=weights,
            weight_indices=torch.tensor([0], dtype=torch.int32),
            seg_len_tensor=torch.tensor([1], dtype=torch.int32),
            lora_ranks=torch.tensor([0], dtype=torch.int32),
            slice_offsets=torch.tensor([0, 2], dtype=torch.int32),
            base_output=base_output.clone(),
        )

        self.assertTrue(torch.isfinite(output).all())
        torch.testing.assert_close(output, base_output)

    def test_backend_rank_override_restores_original_batch_info(self):
        @dataclass
        class FakeBatchInfo:
            lora_ranks: torch.Tensor
            lora_ranks_cpu: torch.Tensor
            marker: int

        class FakeBackend:
            @contextmanager
            def use_lora_ranks(self, lora_ranks, lora_ranks_cpu=None):
                old_batch_info = self.batch_info
                updates = {"lora_ranks": lora_ranks}
                if lora_ranks_cpu is not None:
                    updates["lora_ranks_cpu"] = lora_ranks_cpu
                self.batch_info = replace(self.batch_info, **updates)
                try:
                    yield
                finally:
                    self.batch_info = old_batch_info

        backend = FakeBackend()
        original_ranks = torch.tensor([4], dtype=torch.int32)
        original_ranks_cpu = torch.tensor([4], dtype=torch.int32)
        backend.batch_info = FakeBatchInfo(original_ranks, original_ranks_cpu, marker=7)

        module_ranks = torch.tensor([0], dtype=torch.int32)
        module_ranks_cpu = torch.tensor([0], dtype=torch.int32)
        with backend.use_lora_ranks(module_ranks, module_ranks_cpu):
            self.assertIs(backend.batch_info.lora_ranks, module_ranks)
            self.assertIs(backend.batch_info.lora_ranks_cpu, module_ranks_cpu)
            self.assertEqual(backend.batch_info.marker, 7)

        self.assertIs(backend.batch_info.lora_ranks, original_ranks)
        self.assertIs(backend.batch_info.lora_ranks_cpu, original_ranks_cpu)

    def test_module_active_check_uses_only_real_lora_slots(self):
        class FakeLayer:
            def has_active_lora_for_current_batch(self, batch_info=None):
                if getattr(batch_info, "use_cuda_graph", False):
                    return True
                active_weight_indices = getattr(batch_info, "active_weight_indices", None)
                if active_weight_indices is None:
                    return True
                return any(
                    self.lora_ranks_cpu[idx].item() > 0
                    for idx in active_weight_indices
                )

        @dataclass
        class FakeBatchInfo:
            active_weight_indices: tuple[int, ...]
            use_cuda_graph: bool = False

        layer = FakeLayer()
        layer.lora_ranks_cpu = torch.tensor([0, 4], dtype=torch.int32)

        self.assertFalse(
            layer.has_active_lora_for_current_batch(FakeBatchInfo(tuple()))
        )
        self.assertFalse(
            layer.has_active_lora_for_current_batch(FakeBatchInfo((0,)))
        )
        self.assertTrue(
            layer.has_active_lora_for_current_batch(FakeBatchInfo((1,)))
        )
        self.assertTrue(
            layer.has_active_lora_for_current_batch(
                FakeBatchInfo(tuple(), use_cuda_graph=True)
            )
        )


if __name__ == "__main__":
    unittest.main()
