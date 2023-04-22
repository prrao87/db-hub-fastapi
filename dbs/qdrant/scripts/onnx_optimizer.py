import os
from pathlib import Path

from optimum.onnxruntime import ORTModelForCustomTasks, ORTOptimizer, ORTQuantizer
from optimum.onnxruntime.configuration import OptimizationConfig, AutoQuantizationConfig
from optimum.pipelines import pipeline
from transformers import AutoTokenizer


def convert_to_onnx(model_id: str, output_dir: str) -> Path:
    """
    Download Hugging Face model checkpoint and tokenizer, and then
    convert to ONNX format and save to disk
    """
    model = ORTModelForCustomTasks.from_pretrained(model_id, export=True)
    tokenizer = AutoTokenizer.from_pretrained(model_id)
    # Save to local directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    onnx_path = Path(output_dir)
    model.save_pretrained(onnx_path)
    tokenizer.save_pretrained(onnx_path)
    return onnx_path


def optimize_onnx_model(onnx_path: Path) -> None:
    """
    Optimize ONNX model for CPU performance
    """
    # Create ORTOptimizer and define optimization configuration
    optimizer = ORTOptimizer.from_pretrained(onnx_path)
    # Set optimization_level = 99 -> enable all optimizations
    optimization_config = OptimizationConfig(optimization_level=99)
    # Apply the optimization configuration to the model
    optimizer.optimize(
        optimization_config=optimization_config,
        save_dir=onnx_path,
    )


def quantize_optimized_onnx_model(onnx_path: Path) -> None:
    """
    Quantize an already optimized ONNX model for even better CPU performance
    """
    # Create ORTQuantizer and define quantization configuration
    quantizer = ORTQuantizer.from_pretrained(onnx_path, file_name="model_optimized.onnx")
    quantization_config = AutoQuantizationConfig.avx512_vnni(is_static=False, per_channel=True)
    # Apply the quantization configuration to the model
    quantizer.quantize(
        quantization_config=quantization_config,
        save_dir=onnx_path,
    )


def get_embedding_pipeline(onnx_path, model_filename: str) -> pipeline:
    """
    Create a sentence embedding pipeline using the optimized ONNX model
    """
    # Reload tokenizer
    tokenizer = AutoTokenizer.from_pretrained(onnx_path)
    optimized_model = ORTModelForCustomTasks.from_pretrained(onnx_path, file_name=model_filename)
    embedding_pipeline = pipeline("feature-extraction", model=optimized_model, tokenizer=tokenizer)
    return embedding_pipeline


def main(embedding_pipeline: pipeline, text: str) -> None:
    """
    Generate sentence embeddings for the given text using optimized ONNX model
    """
    embedding = embedding_pipeline(text)[0][0]
    print(embedding[:10])
    print(f"Generated embedding of length {len(embedding)} from '{model_id}'")


if __name__ == "__main__":
    text = "This is a fabulous wine with a smooth and fruity finish."
    model_id = os.environ.get(
        "EMBEDDING_MODEL_CHECKPOINT",
        "sentence-transformers/multi-qa-MiniLM-L6-cos-v1",
    )
    output_dir = "onnx_models"
    onnx_path = convert_to_onnx(model_id, output_dir)
    # First, optimize the ONNX model
    optimize_onnx_model(onnx_path)
    # Next, quantize the optimized ONNX model
    quantize_optimized_onnx_model(onnx_path)
    embedding_pipeline = get_embedding_pipeline(
        onnx_path, model_filename="model_optimized_quantized.onnx"
    )
    main(embedding_pipeline, text)
