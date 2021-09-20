package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/forecast"
	"github.com/aws/aws-sdk-go-v2/service/forecast/types"
	"github.com/aws/aws-sdk-go-v2/service/forecastquery"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

func main() {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}

	svc := forecast.NewFromConfig(cfg)
	query := forecastquery.NewFromConfig(cfg)
	caller, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	f := Forecast{region: cfg.Region, svc: svc, query: query, caller: caller}

	log.Println("create dataset")
	datasetArn, err := f.CreateDataset(ctx, "electricityusagedata")
	if err != nil {
		log.Fatal(err)
	}

	log.Println("create importDatasetJob")
	datasetImportJobArn, err := f.CreateDatasetImportJob(ctx, "import_electricityusagedata",
		"electricityusagedata", *datasetArn, &types.S3Config{
			Path:    aws.String("s3://hogefuga/electricityusagedata.csv"),
			RoleArn: aws.String("arn:aws:iam::524580158183:role/service-role/AmazonForecast-ExecutionRole-1613821137643"),
		})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("create datasetGroup")
	datasetGroupArn, err := f.CreateDatasetGroup(ctx, "electricityusagedatagroup", []string{*datasetArn})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("create predictor")
	predictorArn, err := f.CreatePredictor(ctx, "electricityusagedata_predictor", *datasetGroupArn)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("create forecast")
	forecastArn, err := f.CreateForecast(ctx, "electricityusagedata_forecast", *predictorArn)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("create forecastExportJob")
	forecastExportJobArn, err := f.CreateForecastExportJob(ctx, "export_electricityusagedata_forecast",
		"electricityusagedata_forecast", *forecastArn, &types.S3Config{
			Path:    aws.String("s3://hogefuga/electricityusagedata_forecast/"),
			RoleArn: aws.String("arn:aws:iam::524580158183:role/service-role/AmazonForecast-ExecutionRole-1613821137643"),
		})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("clean up")
	if err := f.DeleteForecastExportJob(ctx, *forecastExportJobArn); err != nil {
		log.Fatal(err)
	}
	if err := f.DeleteForecast(ctx, *forecastArn); err != nil {
		log.Fatal(err)
	}
	if err := f.DeletePredictor(ctx, *predictorArn); err != nil {
		log.Fatal(err)
	}

	if err := f.DeleteDatasetGroup(ctx, *datasetGroupArn); err != nil {
		log.Fatal(err)
	}
	if err := f.DeleteDatasetImportJob(ctx, *datasetImportJobArn); err != nil {
		log.Fatal(err)
	}
	if err := f.DeleteDataset(ctx, *datasetArn); err != nil {
		log.Fatal(err)
	}
}

type Forecast struct {
	region string
	svc    *forecast.Client
	query  *forecastquery.Client
	caller *sts.GetCallerIdentityOutput
}

func (f Forecast) skipIfAlreadyExists(resource string, name string, h func() (*string, error)) (*string, error) {
	arn, err := h()
	if err != nil {
		var exists *types.ResourceAlreadyExistsException
		if errors.As(err, &exists) {
			log.Printf("skip to create %s already exists", resource)
			return aws.String(fmt.Sprintf("arn:aws:forecast:%s:%s:%s/%s", f.region, *f.caller.Account, resource, name)), nil
		}
		return nil, err
	}
	return arn, nil
}

func (f Forecast) waitForActive(ctx context.Context, name string, h func() (*string, *int64, error)) error {
	ticker := time.NewTicker(time.Minute * 1)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			status, remainingMin, err := h()
			if err != nil {
				return err
			}
			if *status == "ACTIVE" {
				return nil
			} else if !strings.HasPrefix(*status, "CREATE") {
				return fmt.Errorf("%s is not creating but %s", name, *status)
			} else if *status == "CREATE_FAILED" {
				return errors.New("creating is failed")
			}
			if remainingMin != nil {
				log.Printf("%s's status is %s. remaining %d mins", name, *status, *remainingMin)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (f Forecast) CreateDataset(ctx context.Context, name string) (*string, error) {
	return f.skipIfAlreadyExists("dataset", name, func() (*string, error) {
		dataset, err := f.svc.CreateDataset(ctx, &forecast.CreateDatasetInput{
			DatasetName:   aws.String(name),
			DatasetType:   types.DatasetTypeTargetTimeSeries,
			DataFrequency: aws.String("H"),
			Domain:        types.DomainCustom,
			Schema: &types.Schema{
				Attributes: []types.SchemaAttribute{
					{
						AttributeName: aws.String("timestamp"),
						AttributeType: types.AttributeTypeTimestamp,
					},
					{
						AttributeName: aws.String("target_value"),
						AttributeType: types.AttributeTypeFloat,
					},
					{
						AttributeName: aws.String("item_id"),
						AttributeType: types.AttributeTypeString,
					},
				},
			},
		})
		if err != nil {
			return nil, err
		}
		return dataset.DatasetArn, nil
	})
}

func (f Forecast) CreateDatasetImportJob(ctx context.Context, name, datasetName, datasetArn string, src *types.S3Config) (*string, error) {
	arn, err := f.skipIfAlreadyExists(fmt.Sprintf("dataset-import-job/%s", datasetName), name, func() (*string, error) {
		job, err := f.svc.CreateDatasetImportJob(ctx, &forecast.CreateDatasetImportJobInput{
			DatasetImportJobName: aws.String(name),
			DatasetArn:           &datasetArn,
			TimeZone:             aws.String("America/Los_Angeles"),
			DataSource: &types.DataSource{
				S3Config: src,
			},
		})
		if err != nil {
			return nil, err
		}
		return job.DatasetImportJobArn, nil
	})
	if err != nil {
		return nil, err
	}

	if err := f.waitForActive(ctx, "dataset-import-job", func() (*string, *int64, error) {
		desc, err := f.svc.DescribeDatasetImportJob(ctx, &forecast.DescribeDatasetImportJobInput{
			DatasetImportJobArn: arn,
		})
		if err != nil {
			return nil, nil, err
		}
		return desc.Status, desc.EstimatedTimeRemainingInMinutes, nil
	}); err != nil {
		return nil, err
	}
	return arn, nil
}

func (f Forecast) CreateDatasetGroup(ctx context.Context, name string, datasetArns []string) (*string, error) {
	return f.skipIfAlreadyExists("dataset-group", name, func() (*string, error) {
		datasetGroup, err := f.svc.CreateDatasetGroup(ctx, &forecast.CreateDatasetGroupInput{
			DatasetGroupName: aws.String(name),
			DatasetArns:      datasetArns,
			Domain:           types.DomainCustom,
		})
		if err != nil {
			return nil, err
		}
		return datasetGroup.DatasetGroupArn, nil
	})
}

func (f Forecast) CreatePredictor(ctx context.Context, name, datasetGroupArn string) (*string, error) {
	arn, err := f.skipIfAlreadyExists("predictor", name, func() (*string, error) {
		predictor, err := f.svc.CreatePredictor(ctx, &forecast.CreatePredictorInput{
			PredictorName:   aws.String(name),
			ForecastHorizon: aws.Int32(72), // 3 days 2015-01-01T00:00:00 - 2015-01-04T00:00:00
			FeaturizationConfig: &types.FeaturizationConfig{
				ForecastFrequency: aws.String("H"),
			},
			PerformAutoML: aws.Bool(true),
			InputDataConfig: &types.InputDataConfig{
				DatasetGroupArn: aws.String(datasetGroupArn),
				SupplementaryFeatures: []types.SupplementaryFeature{
					{
						Name:  aws.String("holiday"),
						Value: aws.String("US"),
					},
				},
			},
		})
		if err != nil {
			return nil, err
		}
		return predictor.PredictorArn, nil
	})
	if err != nil {
		return nil, err
	}

	if err := f.waitForActive(ctx, "predictor", func() (*string, *int64, error) {
		desc, err := f.svc.DescribePredictor(ctx, &forecast.DescribePredictorInput{
			PredictorArn: arn,
		})
		if err != nil {
			return nil, nil, err
		}
		return desc.Status, desc.EstimatedTimeRemainingInMinutes, nil
	}); err != nil {
		return nil, err
	}
	return arn, err
}

func (f Forecast) CreateForecast(ctx context.Context, name, predictorArn string) (*string, error) {
	arn, err := f.skipIfAlreadyExists("forecast", name, func() (*string, error) {
		forecast, err := f.svc.CreateForecast(ctx, &forecast.CreateForecastInput{
			ForecastName: aws.String(name),
			PredictorArn: aws.String(predictorArn),
		})
		if err != nil {
			return nil, err
		}
		return forecast.ForecastArn, nil
	})
	if err != nil {
		return nil, err
	}

	if err := f.waitForActive(ctx, "forecast", func() (*string, *int64, error) {
		desc, err := f.svc.DescribeForecast(ctx, &forecast.DescribeForecastInput{
			ForecastArn: arn,
		})
		if err != nil {
			return nil, nil, err
		}
		return desc.Status, desc.EstimatedTimeRemainingInMinutes, nil
	}); err != nil {
		return nil, err
	}
	return arn, nil
}

func (f Forecast) CreateForecastExportJob(ctx context.Context, name, forecastName, forecastArn string, dest *types.S3Config) (*string, error) {
	arn, err := f.skipIfAlreadyExists(fmt.Sprintf("forecast-export-job/%s", forecastName), name, func() (*string, error) {
		job, err := f.svc.CreateForecastExportJob(ctx, &forecast.CreateForecastExportJobInput{
			ForecastExportJobName: aws.String(name),
			ForecastArn:           aws.String(forecastArn),
			Destination: &types.DataDestination{
				S3Config: dest,
			},
		})
		if err != nil {
			return nil, err
		}
		return job.ForecastExportJobArn, nil
	})
	if err != nil {
		return nil, err
	}

	if f.waitForActive(ctx, "forecast-export-job", func() (*string, *int64, error) {
		desc, err := f.svc.DescribeForecastExportJob(ctx, &forecast.DescribeForecastExportJobInput{
			ForecastExportJobArn: arn,
		})
		if err != nil {
			return nil, nil, err
		}
		return desc.Status, aws.Int64(0), nil
	}); err != nil {
		return nil, err
	}
	return arn, nil
}

func (f Forecast) waitForDeleted(ctx context.Context, name string, h func() (*string, error)) error {
	ticker := time.NewTicker(time.Minute * 1)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			status, err := h()
			if err != nil {
				var notFound *types.ResourceNotFoundException
				if errors.As(err, &notFound) {
					return nil
				}
				return err
			}
			if !strings.HasPrefix(*status, "DELETE") {
				return fmt.Errorf("%s is not deleting but %s", name, *status)
			} else if *status == "DELTE_FAILED" {
				return errors.New("deleting is failed")
			}
			log.Printf("%s's status is %s", name, *status)
		case <-ctx.Done():
			return nil
		}
	}
}

func (f Forecast) DeleteForecastExportJob(ctx context.Context, forecastExportJobArn string) error {
	_, err := f.svc.DeleteForecastExportJob(ctx, &forecast.DeleteForecastExportJobInput{
		ForecastExportJobArn: aws.String(forecastExportJobArn),
	})
	if err != nil {
		return err
	}

	return f.waitForDeleted(ctx, "forecast-export-job", func() (*string, error) {
		desc, err := f.svc.DescribeForecastExportJob(ctx, &forecast.DescribeForecastExportJobInput{
			ForecastExportJobArn: aws.String(forecastExportJobArn),
		})
		if err != nil {
			return nil, err
		}
		return desc.Status, nil
	})
}

func (f Forecast) DeleteForecast(ctx context.Context, forecastArn string) error {
	_, err := f.svc.DeleteForecast(ctx, &forecast.DeleteForecastInput{
		ForecastArn: aws.String(forecastArn),
	})
	if err != nil {
		return err
	}

	return f.waitForDeleted(ctx, "forecast", func() (*string, error) {
		desc, err := f.svc.DescribeForecast(ctx, &forecast.DescribeForecastInput{
			ForecastArn: aws.String(forecastArn),
		})
		if err != nil {
			return nil, err
		}
		return desc.Status, nil
	})
}

func (f Forecast) DeletePredictor(ctx context.Context, predictorArn string) error {
	_, err := f.svc.DeletePredictor(ctx, &forecast.DeletePredictorInput{
		PredictorArn: aws.String(predictorArn),
	})
	if err != nil {
		return err
	}

	return f.waitForDeleted(ctx, "predictor", func() (*string, error) {
		desc, err := f.svc.DescribePredictor(ctx, &forecast.DescribePredictorInput{
			PredictorArn: aws.String(predictorArn),
		})
		if err != nil {
			return nil, err
		}
		return desc.Status, nil
	})
}

func (f Forecast) DeleteDatasetGroup(ctx context.Context, datasetGroupArn string) error {
	_, err := f.svc.DeleteDatasetGroup(ctx, &forecast.DeleteDatasetGroupInput{
		DatasetGroupArn: aws.String(datasetGroupArn),
	})
	if err != nil {
		return err
	}
	return nil
}

func (f Forecast) DeleteDatasetImportJob(ctx context.Context, datasetImportJobArn string) error {
	_, err := f.svc.DeleteDatasetImportJob(ctx, &forecast.DeleteDatasetImportJobInput{
		DatasetImportJobArn: aws.String(datasetImportJobArn),
	})
	if err != nil {
		return err
	}

	return f.waitForDeleted(ctx, "dataset-import-job", func() (*string, error) {
		desc, err := f.svc.DescribeDatasetImportJob(ctx, &forecast.DescribeDatasetImportJobInput{
			DatasetImportJobArn: aws.String(datasetImportJobArn),
		})
		if err != nil {
			return nil, err
		}
		return desc.Status, nil
	})
}

func (f Forecast) DeleteDataset(ctx context.Context, datasetArn string) error {
	_, err := f.svc.DeleteDataset(ctx, &forecast.DeleteDatasetInput{
		DatasetArn: aws.String(datasetArn),
	})
	if err != nil {
		return err
	}
	return nil
}
