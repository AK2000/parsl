import parsl
import pytest


@parsl.python_app
def train_and_test():
    import tensorflow as tf
    import tensorflow_datasets as tfds

    tfds.folder_dataset.ImageFolder('/lus/grand/projects/CSC249ADCD08/vhayot/gpu-burn/td')

    (ds_train, ds_test), ds_info = tfds.load(
                    'mnist',
                    split=['train', 'test'],
                    shuffle_files=True,
                    as_supervised=True,
                    with_info=True,
                    data_dir='/lus/grand/projects/CSC249ADCD08/vhayot/gpu-burn/td',
    )



    def normalize_img(image, label):
        """Normalizes images: `uint8` -> `float32`."""
        return tf.cast(image, tf.float32) / 255., label

    ds_train = ds_train.map( normalize_img, num_parallel_calls=tf.data.AUTOTUNE)
    ds_train = ds_train.cache()
    ds_train = ds_train.shuffle(ds_info.splits['train'].num_examples)

    ds_train = ds_train.batch(128)
    ds_train = ds_train.prefetch(tf.data.AUTOTUNE)


    ds_test = ds_test.map(
                    normalize_img, num_parallel_calls=tf.data.AUTOTUNE)
    ds_test = ds_test.batch(128)
    ds_test = ds_test.cache()
    ds_test = ds_test.prefetch(tf.data.AUTOTUNE)



    model = tf.keras.models.Sequential([
        tf.keras.layers.Flatten(input_shape=(28, 28)),
        tf.keras.layers.Dense(128, activation='relu'),
        tf.keras.layers.Dense(10)
    ])
    model.compile(
            optimizer=tf.keras.optimizers.Adam(0.001),
            loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
            metrics=[tf.keras.metrics.SparseCategoricalAccuracy()],
    )

    model.fit(
            ds_train,
            epochs=6,
            validation_data=ds_test,
    )

    return 0

@pytest.mark.local
def test_gpu():
    from parsl.tests.configs.htex_local_energy import fresh_config
    parsl.load(fresh_config())
    train_and_test().result()
    assert True


