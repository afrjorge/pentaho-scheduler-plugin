/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/
package org.pentaho.platform.plugin.services.importer;

import org.apache.commons.collections.CollectionUtils;

import org.pentaho.platform.api.importexport.IImportHelper;
import org.pentaho.platform.api.importexport.ImportException;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.api.scheduler2.IJob;
import org.pentaho.platform.api.scheduler2.IJobRequest;
import org.pentaho.platform.api.scheduler2.IJobScheduleParam;
import org.pentaho.platform.api.scheduler2.IJobScheduleRequest;
import org.pentaho.platform.api.scheduler2.IJobTrigger;
import org.pentaho.platform.api.scheduler2.IScheduler;
import org.pentaho.platform.api.scheduler2.ISchedulerResource;
import org.pentaho.platform.api.scheduler2.JobState;
import org.pentaho.platform.api.scheduler2.SimpleJobTrigger;
import org.pentaho.platform.engine.core.system.PentahoSystem;
import org.pentaho.platform.plugin.services.importexport.ImportSession;
import org.pentaho.platform.plugin.services.messages.Messages;

import jakarta.ws.rs.core.Response;
import org.pentaho.platform.scheduler2.quartz.QuartzScheduler;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Imports schedule definitions from the active platform import session.
 * <p>
 * The scheduler is paused while schedules are inspected, replaced, and created. Once paused, a restart is attempted
 * regardless of whether the import completes successfully. A restart failure is propagated unless another import
 * failure is already in flight, in which case it is attached to the original failure as a suppressed exception.
 */
public class ScheduleImportUtil implements IImportHelper {
  private static final String RESERVEDMAPKEY_LINEAGE_ID = "lineage-id";
  private static final String SCHEDULE_IMPORT_UTIL_NAME ="schedule-import-util";

  public ScheduleImportUtil() {
    super();
  }

  public void registerAsHelper() {
    PentahoSystem.get( SolutionImportHandler.class, "solutionImportHandler", null ).addImportHelper( this );
  }

  /**
   * Imports all schedules from the active import manifest.
   * <p>
   * Individual schedule-creation failures are logged with their causes and do not stop the remaining schedules.
   * Failures outside that per-schedule creation boundary are propagated after an attempt to restart the scheduler; a
   * restart failure is then attached to them as a suppressed exception rather than replacing them.
   *
   * @param solutionImportHandler the import context supplying overwrite behavior and logging
   * @throws ImportException if the import framework reports a checked import failure
   */
  @Override
  public void doImport( IImportHelper.ImportContext solutionImportHandler ) throws ImportException {
    List<IJobScheduleRequest> scheduleList = getScheduleList();
    if ( solutionImportHandler.isPerformingRestore() ) {
      solutionImportHandler.getLogger().info( Messages.getInstance().getString( "SolutionImportHandler.INFO_START_IMPORT_SCHEDULE" ) );
    }
    if ( CollectionUtils.isNotEmpty( scheduleList ) ) {
      if ( solutionImportHandler.isPerformingRestore() ) {
        solutionImportHandler.getLogger().info( Messages.getInstance().getString( "SolutionImportHandler.INFO_COUNT_SCHEDULUE", scheduleList.size() ) );
      }
      int successfulScheduleImportCount = 0;
      IScheduler scheduler = PentahoSystem.get( IScheduler.class, "IScheduler2", null ); //$NON-NLS-1$
      ISchedulerResource schedulerResource = scheduler.createSchedulerResource();
      if ( solutionImportHandler.isPerformingRestore() ) {
        solutionImportHandler.getLogger().debug( "Pausing the scheduler before the start of the restore process" );
      }
      schedulerResource.pause();
      if ( solutionImportHandler.isPerformingRestore() ) {
        solutionImportHandler.getLogger().debug( "Successfully paused the scheduler" );
      }
      Throwable importFailure = null;
      try {
        for ( IJobScheduleRequest jobScheduleRequest : scheduleList ) {
          if ( solutionImportHandler.isPerformingRestore() ) {
            solutionImportHandler.getLogger().debug( "Restoring schedule name [ " + jobScheduleRequest.getJobName() + "] inputFile [ " + jobScheduleRequest.getInputFile() + " ] outputFile [ " + jobScheduleRequest.getOutputFile() + "]" );
          }
          boolean jobExists = false;

          List<IJob> jobs = schedulerResource.getJobsList();
          if ( jobs != null ) {

            //paramRequest to map<String, Serializable>
            Map<String, Serializable> mapParamsRequest = new HashMap<>();
            for ( IJobScheduleParam paramRequest : jobScheduleRequest.getJobParameters() ) {
              mapParamsRequest.put( paramRequest.getName(), paramRequest.getValue() );
            }

            // We will check the existing job in the repository. If the job being imported exists, we will remove it from the repository
            for ( IJob job : jobs ) {

              if ( ( mapParamsRequest.get( RESERVEDMAPKEY_LINEAGE_ID ) != null )
                && ( mapParamsRequest.get( RESERVEDMAPKEY_LINEAGE_ID )
                .equals( job.getJobParams().get( RESERVEDMAPKEY_LINEAGE_ID ) ) ) ) {
                jobExists = true;
              }

              if ( solutionImportHandler.isOverwriteFile() && jobExists ) {
                if ( solutionImportHandler.isPerformingRestore() ) {
                  solutionImportHandler.getLogger().debug( "Schedule  [ " + jobScheduleRequest.getJobName() + "] already exists and overwrite flag is set to true. Removing the job so we can add it again" );
                }
                IJobRequest jobRequest = scheduler.createJobRequest();
                jobRequest.setJobId( job.getJobId() );
                schedulerResource.removeJob( jobRequest );
                jobExists = false;
                break;
              }
            }
          }

          boolean canImport = convertFromPreTimeZoneTrigger( jobScheduleRequest, solutionImportHandler );
          if ( !canImport ) {
            continue;
          }

          if ( !jobExists ) {
            try {
              Response response = createSchedulerJob( schedulerResource, jobScheduleRequest );
              if ( response.getStatus() == Response.Status.OK.getStatusCode() ) {
                if ( response.getEntity() != null ) {
                  // get the schedule job id from the response and add it to the import session
                  ImportSession.getSession().addImportedScheduleJobId( response.getEntity().toString() );
                  if ( solutionImportHandler.isPerformingRestore() ) {
                    solutionImportHandler.getLogger().debug( "Successfully restored schedule [ " + jobScheduleRequest.getJobName() + " ] " );
                  }
                  successfulScheduleImportCount++;
                }
              } else {
                solutionImportHandler.getLogger().error( Messages.getInstance().getString( "SolutionImportHandler.ERROR_IMPORTING_SCHEDULE", jobScheduleRequest.getJobName(), response.getEntity() != null
                  ? response.getEntity().toString() : "" ) );
              }
            } catch ( Exception e ) {
              // there is a scenario where if the file scheduled has a space in the file name, that it won't work. the
              // di server

              // replaces spaces with underscores and the export mechanism can't determine if it needs this to happen
              // or not
              // so, if we failed to import and there is a space in the path, try again but this time with replacing
              // the space(s)
              if ( jobScheduleRequest.getInputFile().contains( " " ) || jobScheduleRequest.getOutputFile()
                .contains( " " ) ) {
                solutionImportHandler.getLogger().debug( Messages.getInstance()
                  .getString( "SolutionImportHandler.SchedulesWithSpaces", jobScheduleRequest.getInputFile() ) );
                File inFile = new File( jobScheduleRequest.getInputFile() );
                File outFile = new File( jobScheduleRequest.getOutputFile() );
                String inputFileName = inFile.getParent() + RepositoryFile.SEPARATOR
                  + inFile.getName().replace( " ", "_" );
                String outputFileName = outFile.getParent() + RepositoryFile.SEPARATOR
                  + outFile.getName().replace( " ", "_" );
                jobScheduleRequest.setInputFile( inputFileName );
                jobScheduleRequest.setOutputFile( outputFileName );
                try {
                  if ( !File.separator.equals( RepositoryFile.SEPARATOR ) ) {
                    // on windows systems, the backslashes will result in the file not being found in the repository
                    jobScheduleRequest.setInputFile( inputFileName.replace( File.separator, RepositoryFile.SEPARATOR ) );
                    jobScheduleRequest
                      .setOutputFile( outputFileName.replace( File.separator, RepositoryFile.SEPARATOR ) );
                  }
                  Response response = createSchedulerJob( schedulerResource, jobScheduleRequest );
                  if ( response.getStatus() == Response.Status.OK.getStatusCode() ) {
                    if ( response.getEntity() != null ) {
                      // get the schedule job id from the response and add it to the import session
                      ImportSession.getSession().addImportedScheduleJobId( response.getEntity().toString() );
                      successfulScheduleImportCount++;
                    }
                  }
                } catch ( Exception ex ) {
                  // log it and keep going. we shouldn't stop processing all schedules just because one fails.
                  solutionImportHandler.getLogger().error( Messages.getInstance()
                    .getString( "SolutionImportHandler.ERROR_0001_ERROR_CREATING_SCHEDULE", "[ " + jobScheduleRequest.getJobName() + " ] cause [ " + ex.getMessage() + " ]" ), ex );
                }
              } else {
                // log it and keep going. we shouldn't stop processing all schedules just because one fails.
                solutionImportHandler.getLogger().error( Messages.getInstance()
                  .getString( "SolutionImportHandler.ERROR_0001_ERROR_CREATING_SCHEDULE", "[ " + jobScheduleRequest.getJobName() + " ] cause [ " + e + " ]" ), e );
              }
            }
          } else {
            solutionImportHandler.getLogger().info( Messages.getInstance()
              .getString( "DefaultImportHandler.ERROR_0009_OVERWRITE_CONTENT", jobScheduleRequest.toString() ) );
          }
        }
        if ( solutionImportHandler.isPerformingRestore() ) {
          solutionImportHandler.getLogger().info( Messages.getInstance()
            .getString( "SolutionImportHandler.INFO_SUCCESSFUL_SCHEDULE_IMPORT_COUNT", successfulScheduleImportCount, scheduleList.size() ) );
        }
      } catch ( Throwable t ) {
        // Recorded so that the finally block can attach a restart failure instead of masking this one. Throwable is
        // required because a checked ImportException or an Error must not be discarded either.
        importFailure = t;
        throw t;
      } finally {
        boolean schedulerRestarted = false;
        try {
          // Only the restart itself is guarded. Reporting it must not be able to masquerade as a restart failure.
          schedulerResource.start();
          schedulerRestarted = true;
        } catch ( Throwable restartFailure ) {
          // Mirrors the guard above: an Error raised while restarting must not replace the in-flight import failure.
          // The suppression decision is taken before logging, because the import logger can itself fail in exactly
          // these sequences and must not be able to throw out of this block and mask importFailure.
          if ( importFailure != null ) {
            importFailure.addSuppressed( restartFailure );
          }
          logRestartFailure( solutionImportHandler, restartFailure );
          if ( importFailure == null ) {
            throw restartFailure;
          }
        }
        if ( schedulerRestarted ) {
          logSchedulerRestarted( solutionImportHandler );
        }
      }
    }
    if ( solutionImportHandler.isPerformingRestore() ) {
      solutionImportHandler.getLogger().info( Messages.getInstance().getString( "SolutionImportHandler.INFO_END_IMPORT_SCHEDULE" ) );
    }
  }

  /**
   * Logs a scheduler restart failure without ever throwing.
   * <p>
   * This is called from a {@code finally} block while another failure may already be propagating. The import logger
   * writes to the restore log, which can itself be unusable in precisely those failure sequences, so a logging failure
   * is attached to the restart failure rather than allowed to escape and mask the original one.
   *
   * @param solutionImportHandler the import context supplying the logger
   * @param restartFailure the scheduler restart failure to report
   */
  private void logRestartFailure( IImportHelper.ImportContext solutionImportHandler, Throwable restartFailure ) {
    try {
      solutionImportHandler.getLogger().error(
        Messages.getInstance().getString( "SolutionImportHandler.ERROR_0002_SCHEDULER_RESTART_FAILED" ),
        restartFailure );
    } catch ( Throwable loggingFailure ) {
      if ( loggingFailure != restartFailure ) {
        restartFailure.addSuppressed( loggingFailure );
      }
    }
  }

  /**
   * Reports a successful scheduler restart without ever throwing.
   * <p>
   * This runs in a {@code finally} block, after the restart guard, while another failure may still be propagating. A
   * failure of the import logger must therefore neither mask that failure nor turn a completed restore into a failed
   * one, so it is deliberately swallowed: the logger is the very thing that failed, leaving nowhere to report it.
   *
   * @param solutionImportHandler the import context supplying the logger
   */
  private void logSchedulerRestarted( IImportHelper.ImportContext solutionImportHandler ) {
    if ( !solutionImportHandler.isPerformingRestore() ) {
      return;
    }
    try {
      solutionImportHandler.getLogger().debug( "Successfully started the scheduler" );
    } catch ( Throwable loggingFailure ) {
      // Intentionally ignored - see the method javadoc.
    }
  }

  public Response createSchedulerJob( ISchedulerResource scheduler, IJobScheduleRequest jobScheduleRequest )
    throws IOException {
    Response rs = scheduler != null ? (Response) scheduler.createJob( jobScheduleRequest ) : null;
    if ( jobScheduleRequest.getJobState() != JobState.NORMAL ) {
      IJobRequest jobRequest = PentahoSystem.get( IScheduler.class, "IScheduler2", null ).createJobRequest();
      jobRequest.setJobId( rs.getEntity().toString() );
      scheduler.pauseJob( jobRequest );
    }
    return rs;
  }

  protected List<IJobScheduleRequest> getScheduleList() {
    return ImportSession.getSession().getManifest().getScheduleList();
  }

  protected boolean convertFromPreTimeZoneTrigger( IJobScheduleRequest jobScheduleRequest,
                                                               IImportHelper.ImportContext solutionImportHandler ) {
    IJobTrigger jobTrigger = jobScheduleRequest.getSimpleJobTrigger();
    if ( !( jobTrigger instanceof SimpleJobTrigger ) ) {
      // don't skip, nothing to do here
      return true;
    }
    SimpleJobTrigger simpleJobTrigger = (SimpleJobTrigger) jobTrigger;
    if ( triggerIsPreTimeZoneSupport( simpleJobTrigger ) ) {
      // RUN_ONCE triggers are valid with any interval (including 0) and need no conversion
      if ( QuartzScheduler.UI_PASS_PARAM_RUN_ONCE.equalsIgnoreCase( simpleJobTrigger.getUiPassParam() ) ) {
        return true;
      }
      // See if the scheduler will have a problem with this trigger
      int interval = (int) simpleJobTrigger.getRepeatInterval();
      if ( interval <= 0 ) {
        // this would have been a problem even pre-timezone support, report an error
        solutionImportHandler.getLogger().error( Messages.getInstance()
          .getString( "SolutionImportHandler.ERROR_IMPORTING_SCHEDULE", "[ " + jobScheduleRequest.getJobName() + " ]"
            , "Repeat interval <= 0 in import file" ) );
        return false;
      }
      if ( simpleJobTrigger.getUiPassParam() == null ) {
        // Assume SECONDS and log a warning. The trigger needs no further modification since the interval specified
        // will be valid.
        simpleJobTrigger.setUiPassParam( QuartzScheduler.UI_PASS_PARAM_SECONDS );
        solutionImportHandler.getLogger().warn( Messages.getInstance()
          .getString( "SolutionImportHandler.WARN_UI_PASS_PARAM", "[ " + jobScheduleRequest.getJobName() + " ]" ));
      }
      int calculatedInterval = 0;
      try {
        calculatedInterval = QuartzScheduler.calculateTriggerInterval( simpleJobTrigger, interval );
      } catch ( IllegalArgumentException e ) {
        // if we get here, the UiPassParam was a weird value and we should not try to import this trigger.
        solutionImportHandler.getLogger().error( Messages.getInstance()
          .getString( "SolutionImportHandler.ERROR_INVALID_JOB_TRIGGER", "[ " + jobScheduleRequest.getJobName() + " ]"
            , "Unknown UiPassParam: " + simpleJobTrigger.getUiPassParam() ) );
        return false;
      }
      if ( calculatedInterval <= 0 ) {
        // the interval value and the UiPassParam don't align because the number of seconds in the interval is less
        // than the number of seconds in one of that unit; e.g. MINUTES and the interval is less than 60.
        // force this to import as a SECONDS trigger and log a warning
        solutionImportHandler.getLogger().warn( Messages.getInstance()
          .getString( "SolutionImportHandler.WARN_PRE_TIMEZONE_SUPPORT_TRIGGER",
            "[ " + jobScheduleRequest.getJobName() + " ]", interval, simpleJobTrigger.getUiPassParam() ) );
        simpleJobTrigger.setUiPassParam( QuartzScheduler.UI_PASS_PARAM_SECONDS );
      }
    }
    return true;
  }

  protected boolean triggerIsPreTimeZoneSupport( SimpleJobTrigger simpleTrigger ) {
    return simpleTrigger.getStartDay() == -1 && simpleTrigger.getStartMonth() == -1 && simpleTrigger.getStartYear() == -1
      && simpleTrigger.getStartHour() == -1 && simpleTrigger.getStartMin() == -1;
  }

  @Override public String getName() {
    return SCHEDULE_IMPORT_UTIL_NAME;
  }
}
