/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms.windowing;

import com.google.cloud.dataflow.sdk.transforms.DoFn.KeyedState;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;

import org.joda.time.Instant;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * {@code Trigger}s control when the elements for a specific key and window are output. As elements
 * arrive, they are put into one or more windows by the {@code Window} by the {@link WindowFn}, and
 * then passed to the associated {@code Trigger} to determine if the {@code Window}s contents should
 * be output.
 *
 * <p>The elements that are assigned to a window since the last time it was fired (or since the
 * window was created) are placed into a pane. Triggers are evaluated against the elements in the
 * current pane, and when fired, will output those elements. Depending on the trigger, this will
 * either finish the trigger (and the window) or start a new pane.
 *
 * <p>Several predefined {@code Trigger}s are provided:
 * <ul>
 *   <li> {@link AfterWatermark} for firing when the watermark passes a timestamp determined from
 *   either the end of the window or the arrival of the first element in a pane.
 *   <li> {@link AfterProcessingTime} for firing after some amount of processing time has elapsed
 *   (typically since the first element in a pane).
 *   <li> {@link AfterPane} for firing off a property of the elements in the current pane, such as
 *   the number of elements that have been assigned to the current pane.
 * </ul>
 *
 * <p>In addition, {@code Trigger}s can be combined in a variety of ways:
 * <ul>
 *   <li> {@link Repeatedly#forever} to create a trigger that executes forever. Any time its
 *   argument finishes it gets reset and starts over. Can be combined with {@link Repeatedly#until}
 *   to specify a condition which causes the repetition to stop.
 *   <li> {@link AfterEach#inOrder} to execute each trigger in sequence, firing each (and every)
 *   time that a trigger fires, and advancing to the next trigger in the sequence when it finishes.
 *   <li> {@link AfterFirst#of} to create a trigger that fires after at least one of its arguments
 *   fires. An {@link AfterFirst} trigger finishes after it fires once, or when all of its arguments
 *   have finished without firing.
 *   <li> {@link AfterAll#of} to create a trigger that fires after all least one of its arguments
 *   have fired at least once. An {@link AfterFirst} trigger finishes after it fires once, or when
 *   any of its arguments have finished without firing.
 * </ul>
 *
 * <p>Each trigger tree is instantiated per-key and per-window. Every trigger in the tree is in one
 * of the following states:
 * <ul>
 *   <li> Never Existed - before the trigger has started executing, there is no state associated
 *   with it anywhere in the system. A trigger moves to the executing state as soon as it
 *   processes in the current pane.
 *   <li> Executing - while the trigger is receiving items and may fire. While it is in this state,
 *   it may persist book-keeping information to {@link KeyedState}, set timers, etc.
 *   <li> Finished - after a trigger finishes, all of its book-keeping data is cleaned up, and the
 *   system remembers only that it is finished. Entering this state causes us to discard any
 *   elements in the buffer for that window, as well.
 * </ul>
 *
 * <p>Once finished, a trigger cannot return itself back to an earlier state, however a
 * {@link CompositeTrigger} can reset its sub-triggers.
 *
 * <p> This functionality is experimental and likely to change.
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
 *            {@code Trigger}
 */
public interface Trigger<W extends BoundedWindow> extends Serializable {

  /**
   * Triggers operate on both timestamps of elements that are being processed and the current
   * (real-world) time as reported while processing. {@code TimeDomain} specifies which of these
   * domains are applicable to a given operation.
   */
  public enum TimeDomain {
    /**
     * The {@code EVENT_TIME} domain corresponds to the timestamps on the elemnts. Time advances
     * on the system watermark advances.
     */
    EVENT_TIME,

    /**
     * The {@code PROCESSING_TIME} domain corresponds to the current to the current (system) time.
     * This is advanced during exeuction of the Dataflow pipeline.
     */
    PROCESSING_TIME;
  }

  /**
   * {@code WindowStatus} indicates the status of the window that an element is being processed in.
   */
  public enum WindowStatus {
    /**
     * The arrival of this element started a new pane. Either the window is entirely new, or we had
     * previously fired a trigger that caused us to output the earlier elements.
     */
    NEW,

    /** This element was added to a pane that was already being managed. */
    EXISTING,

    /**
     * The window set doesn’t track the windows being managed, so it is not known whether the pane
     * is new. The trigger can track windows on its own if necessary.
     */
    UNKNOWN;
  }

  /**
   * {@code TriggerResult} enumerates the possible result a trigger can have when it is executed.
   */
  public enum TriggerResult {
    FIRE(true, false),
    CONTINUE(false, false),
    FIRE_AND_FINISH(true, true),
    FINISH(false, true);

    private boolean finish;
    private boolean fire;

    private TriggerResult(boolean fire, boolean finish) {
      this.fire = fire;
      this.finish = finish;
    }

    public boolean isFire() {
      return fire;
    }

    public boolean isFinish() {
      return finish;
    }

    public static TriggerResult valueOf(boolean fire, boolean finish) {
      if (fire && finish) {
        return FIRE_AND_FINISH;
      } else if (fire) {
        return FIRE;
      } else if (finish) {
        return FINISH;
      } else {
        return CONTINUE;
      }
    }
  }

  /**
   * Information accessible to all of the callbacks that are executed on a trigger.
   *
   * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
   *            {@code TriggerContext}
   */
  public interface TriggerContext<W extends BoundedWindow>  {

    /**
     * Sets a timer to fire when the watermark or processing time is beyond the given timestamp.
     * Timers are not gauranteed to fire immediately, but will be delivered at some time afterwards.
     *
     * <p>Each trigger can have a single timer in per {@code timeDomain} and {@code window}. If the
     * trigger has already set a timer for a given domain and window, then setting overwrites it.
     *
     * @param window the window the timer is being set for.
     * @param timestamp the time at which the trigger’s {@link Trigger#onTimer} callback should
     *        execute
     * @param timeDomain the domain which the {@code timestamp} applies to
     */
    void setTimer(W window, Instant timestamp, TimeDomain timeDomain) throws IOException;

    /**
     * Removes the timer set in this trigger context for the given {@code window} and
     * {@code timeDomain}.
     */
    void deleteTimer(W window, TimeDomain timeDomain) throws IOException;

    /**
     * Returns the current processing time.
     */
    Instant currentProcessingTime();

    /**
     * Updates the value stored in keyed state for the given {@code tag} and {@code window}.
     */
    <T> void store(CodedTupleTag<T> tag, W window, T value) throws IOException;

    /**
     * Removes the keyed state associated with the given {@code tag} and {@code window}.
     */
    <T> void remove(CodedTupleTag<T> tag, W window) throws IOException;

    /**
     * Lookup the value stored for the given {@code tag} and {@code window}.
     */
    <T> T lookup(CodedTupleTag<T> tag, W window) throws IOException;

    /**
     * Lookup the value stored for a given {@code tag} in a bunch of {@code window}s.
     */
    <T> Map<W, T> lookup(CodedTupleTag<T> tag, Iterable<W> windows) throws IOException;

    /**
     * Create a {@code TriggerContext} for executing a given sub-trigger.
     */
    TriggerContext<W> forChild(int childIndex);
  }

  /**
   * Called immediately after an element is first incorporated into a window.
   *
   * @param c the context to interact with
   * @param value the element that was incorporated
   * @param timestamp the event time that the element arrived at
   * @param window the window the element was assigned to
   */
  TriggerResult onElement(
      TriggerContext<W> c, Object value, Instant timestamp, W
      window, WindowStatus status) throws Exception;

  /**
   * Called immediately after windows have been merged.
   *
   * <p>This will only be called if the trigger hasn't finished in any of the {@code oldWindows}.
   * If it had finished, we assume that it is also finished in the resulting window.
   *
   * <p>The implementation does not need to clear out any state associated with the old windows.
   * That will automatically be done by the trigger execution layer.
   *
   * @param c the context to interact with
   * @param oldWindows the windows that were merged
   * @param newWindow the window that resulted from merging
   */
  TriggerResult onMerge(
      TriggerContext<W> c, Iterable<W> oldWindows, W newWindow) throws Exception;

  /**
   * Called when a timer has fired for the trigger or one of it’s sub-triggers.
   *
   * @param c the context to interact with
   * @param triggerId identifier for the trigger that the timer is for.
   */
  TriggerResult onTimer(
      TriggerContext<W> c, TriggerId<W> triggerId) throws Exception;

  /**
   * Clear any state associated with this trigger in the given window.
   *
   * <p>This is called after a trigger has indicated it will never fire again. The trigger system
   * keeps enough information to know that the trigger is finished, so this trigger should clear all
   * of its state.
   *
   * @param c the context to interact with
   * @param window the window that is being cleared
   */
  void clear(TriggerContext<W> c, W window) throws Exception;

  /**
   * Return true if the trigger is guaranteed to never finish.
   */
  boolean willNeverFinish();

  /**
   * Returns whether this performs the same triggering as the given {@code Trigger}.
   */
  boolean isCompatible(Trigger<?> other);

  /**
   * Identifies a unique trigger instance, by the window it is in and the path through the trigger
   * tree.
   *
   * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
   *            {@code TriggerContext}
   */
  public static class TriggerId<W extends BoundedWindow> {
    private final W window;
    private final List<Integer> subTriggers;

    public TriggerId(W window, List<Integer> subTriggers) {
      this.window = window;
      this.subTriggers = subTriggers;
    }

    /**
     * Return a trigger ID that is applicable for the sub-trigger.
     */
    public TriggerId<W> forChildTrigger() {
      return new TriggerId<>(window, subTriggers.subList(1, subTriggers.size()));
    }

    public W getWindow() {
      return window;
    }

    /**
     * Return true if this trigger ID corresponds to a child of the current trigger.
     */
    public boolean isForChild() {
      return subTriggers.size() > 0;
    }

    /**
     * Return the index of the child this trigger ID is for.
     */
    public int getChildIndex() {
      return subTriggers.get(0);
    }

    public Iterable<Integer> getPath() {
      return subTriggers;
    }
  }

  /**
   * Triggers that are guaranteed to fire at most once should extend from this, rather than the
   * general {@link Trigger} class to indicate that behavior.
   *
   * TODO: Add checks that an AtMostOnceTrigger never returns TriggerResult.FIRE.
   *
   * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
   *            {@code AtMostOnceTrigger}
   */
  public interface AtMostOnceTrigger<W extends BoundedWindow> extends Trigger<W> {}
}
