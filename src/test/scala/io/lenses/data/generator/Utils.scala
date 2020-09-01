package io.lenses.data.generator

import cats.effect.ContextShift
import scala.concurrent.duration.FiniteDuration
import cats.effect.Timer
import cats.effect.IO
import java.util.concurrent.TimeoutException
import scala.util.control.NonFatal

object Utils {
  def retryUntilSuccessful[A](
      action: IO[A],
      timeout: FiniteDuration,
      sleepInterval: FiniteDuration,
      msg: String
  )(
      attemptsLeft: Int = 10
  )(implicit timer: Timer[IO], cs: ContextShift[IO]): IO[A] = {
    if (attemptsLeft <= 0) {
      IO.raiseError(new TimeoutException("retry action timed out! $msg"))
    } else {
      action
        .timeout(timeout)
        .redeemWith(
          {
            case NonFatal(err) =>
              timer.sleep(sleepInterval) *> retryUntilSuccessful(
                action,
                timeout,
                sleepInterval,
                msg
              )(attemptsLeft - 1)
          },
          a => IO.pure(a)
        )

    }

  }

}
